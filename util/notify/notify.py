#!/usr/bin/env python3
"""
References:
1) https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html
2) https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/receive_message.html
3) https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-long-polling.html
4) https://docs.aws.amazon.com/sns/latest/dg/sns-message-and-json-formats.html
5) https://aws.amazon.com/ses/
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/upload_archive.html
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/get_object.html
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/delete_object.html
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/receive_message.html
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/delete_message.html
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/get_item.html
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/update_item.html
https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/scheduler/client/create_schedule.html
"""

import json
import sys
import time
import configparser
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional
from pathlib import Path
import boto3
from botocore.exceptions import ClientError, EndpointConnectionError, BotoCoreError

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))
from helpers import send_email_ses, get_user_profile

def load_config() -> configparser.ConfigParser:
    base_dir = Path(__file__).resolve().parent
    cfg_path = base_dir / "notify_config.ini"
    cfg = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
    read_ok = cfg.read(cfg_path)
    if not read_ok:
        raise FileNotFoundError(f"Could not read config file at {cfg_path!r}")
    return cfg


def epoch_to_local_str(ts: int) -> str:
    return datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M:%S")

# FUnction for parsing SNS message received
def parse_sns_message(body: str) -> Dict[str, Any]:
    outer = json.loads(body)

    if isinstance(outer, dict) and outer.get("Type") == "Notification" and "Message" in outer:
        inner = outer["Message"]
        if isinstance(inner, str):
            return json.loads(inner)
        if isinstance(inner, dict):
            return inner

    if isinstance(outer, dict):
        return outer

    raise ValueError("Unexpected message format")


def receive_messages(
    sqs_client,
    queue_url: str,
    max_msgs: int,
    wait_time: int,
    visibility_timeout: Optional[int],
) -> Dict[str, Any]:
    kwargs: Dict[str, Any] = {
        "QueueUrl": queue_url,
        "MaxNumberOfMessages": min(max(max_msgs, 1), 10),
        "WaitTimeSeconds": min(max(wait_time, 0), 20),
    }
    if visibility_timeout is not None:
        kwargs["VisibilityTimeout"] = int(visibility_timeout)
    return sqs_client.receive_message(**kwargs)


def delete_message(sqs_client, queue_url: str, receipt_handle: str) -> None:
    sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)


# Creating a archive request through event scheduler after 3min
def schedule_archive_request(
    scheduler_client,
    archive_queue_arn: str,
    scheduler_role_arn: str,
    schedule_group: str,
    payload: Dict[str, Any],
) -> None:

    job_id = payload.get("job_id")
    complete_time = int(payload.get("complete_time", int(time.time())))

    # setting time after 3min
    run_at = datetime.fromtimestamp(complete_time, tz=timezone.utc) + timedelta(minutes=3)

    at_expr = f"at({run_at.strftime('%Y-%m-%dT%H:%M:%S')})"

    # Creating a unique name for scheduler
    schedule_name = f"a14-archive-{job_id}"

    archive_msg = {
        "job_id": payload.get("job_id"),
        "user_id": payload.get("user_id"),
        "complete_time": payload.get("complete_time"),
        "s3_results_bucket": payload.get("s3_results_bucket"),
        "s3_key_result_file": payload.get("s3_key_result_file"),
    }

    try:
        scheduler_client.create_schedule(
            Name=schedule_name,
            GroupName=schedule_group,
            ScheduleExpression=at_expr,
            FlexibleTimeWindow={"Mode": "OFF"},
            Target={
                "Arn": archive_queue_arn,
                "RoleArn": scheduler_role_arn,
                "Input": json.dumps(archive_msg),
            },
            ActionAfterCompletion="DELETE",
            Description=f"A14 archive trigger for job {job_id}",
        )
        print(
            f"[notify] Scheduled archive request job_id={job_id} for {run_at.isoformat()} UTC",
            file=sys.stderr,
            flush=True,
        )
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("ConflictException",):
            print(
                f"[notify] Archive schedule already exists for job_id={job_id}; skipping",
                file=sys.stderr,
                flush=True,
            )
            return
        raise


def main() -> int:
    cfg = load_config()

    region = cfg["aws"].get("AwsRegionName", "us-east-1").strip()

    queue_url = cfg["sqs"].get("ResultsQueueUrl", "").strip()
    if not queue_url:
        print("ERROR: Missing ResultsQueueUrl in notify_config.ini", file=sys.stderr)
        return 2

    wait_time = int(cfg["sqs"].get("WaitTime", "20"))
    max_msgs = int(cfg["sqs"].get("MaxMessages", "10"))
    visibility_timeout = None

    sender = cfg["app"].get("Sender", "").strip()
    if not sender or not sender.endswith("@ucmpcs.org"):
        print("ERROR: Sender must be <CNETID>@ucmpcs.org", file=sys.stderr)
        return 2

    base_url = cfg["app"].get("BaseUrl", "").strip().rstrip("/")
    if not base_url:
        print("ERROR: Missing BaseUrl in notify_config.ini", file=sys.stderr)
        return 2

    # A14 scheduler config
    archive_queue_arn = cfg.get("scheduler", "ArchiveQueueArn", fallback="").strip()
    scheduler_role_arn = cfg.get("scheduler", "SchedulerRoleArn", fallback="").strip()
    schedule_group = cfg.get("scheduler", "ScheduleGroup", fallback="default").strip()

    if not archive_queue_arn or not scheduler_role_arn:
        print(
            "ERROR: Missing [scheduler] ArchiveQueueArn and/or SchedulerRoleArn in notify_config.ini",
            file=sys.stderr,
        )
        return 2
    print(f"[notify] Starting. Scheduler={scheduler_role_arn}", file=sys.stderr, flush=True)

    sqs_client = boto3.client("sqs", region_name=region)
    scheduler_client = boto3.client("scheduler", region_name=region)

    print(f"[notify] Starting. Queue={queue_url}", file=sys.stderr, flush=True)
    print(f"[notify] BaseUrl={base_url}", file=sys.stderr, flush=True)

    while True:
        try:
            resp = receive_messages(sqs_client, queue_url, max_msgs, wait_time, visibility_timeout)
        except EndpointConnectionError as e:
            print(f"[notify] receive_message network error: {e}", file=sys.stderr, flush=True)
            time.sleep(2)
            continue
        except (ClientError, BotoCoreError) as e:
            print(f"[notify] receive_message AWS error: {e}", file=sys.stderr, flush=True)
            time.sleep(2)
            continue

        messages = resp.get("Messages", [])
        if not messages:
            continue

        for m in messages:
            receipt = m.get("ReceiptHandle")
            body = m.get("Body", "")

            if not receipt:
                print("[notify] Missing ReceiptHandle; skipping message", file=sys.stderr, flush=True)
                continue

            try:
                payload = parse_sns_message(body)

                job_id = payload.get("job_id")
                user_id = payload.get("user_id")
                complete_time = payload.get("complete_time")

                if not job_id or not user_id or complete_time is None:
                    raise ValueError("Missing required fields: job_id, user_id, complete_time")

                profile = get_user_profile(user_id)
                to_addr = profile.get("email")

                if not to_addr:
                    raise ValueError("User profile missing email address")

                subject = f"Results available for job {job_id}"
                completed_str = epoch_to_local_str(int(complete_time))
                details_link = f"{base_url}/annotations/{job_id}"

                body_text = (
                    f"Your annotation job completed at {completed_str}. "
                    f"Click here to view job details and results: {details_link}."
                )

                send_email_ses(
                    sender=sender,
                    recipients=to_addr,
                    subject=subject,
                    body=body_text,
                )
                # Scheduling archive request for +3 minutes
                schedule_archive_request(
                    scheduler_client=scheduler_client,
                    archive_queue_arn=archive_queue_arn,
                    scheduler_role_arn=scheduler_role_arn,
                    schedule_group=schedule_group,
                    payload=payload,
                )

                delete_message(sqs_client, queue_url, receipt)
                print(f"[notify] Sent email to {to_addr} for job {job_id}; deleted SQS message",
                      file=sys.stderr, flush=True)
                print(
                    f"[notify] Scheduled archive for job {job_id}; deleted SQS message",
                    file=sys.stderr,
                    flush=True,
                )

            except json.JSONDecodeError as e:
                print(f"[notify] Bad JSON; leaving message in queue: {e}", file=sys.stderr, flush=True)
            except ValueError as e:
                print(f"[notify] Invalid payload; leaving message in queue: {e}", file=sys.stderr, flush=True)
            except (ClientError, BotoCoreError) as e:
                print(f"[notify] AWS error; leaving message in queue: {e}", file=sys.stderr, flush=True)
            except Exception as e:
                print(f"[notify] Unexpected error; leaving message in queue: {e}", file=sys.stderr, flush=True)

    # unreachable


if __name__ == "__main__":
    raise SystemExit(main())