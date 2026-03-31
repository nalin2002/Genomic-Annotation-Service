#!/usr/bin/env python3
# thaw_script.py
#
# Thaws upgraded (premium) user data
#
# Copyright (C) 2015-2024 Vas Vasiliadis
# University of Chicago
# https://docs.aws.amazon.com/boto3/latest/reference/services/glacier/client/initiate_job.html
# https://docs.aws.amazon.com/cli/latest/reference/glacier/initiate-job.html
# https://medium.com/@redslick84/how-to-create-a-standard-sqs-queue-using-python-269201539f72
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/receive_message.html
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/delete_message.html
# https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html
# https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs-example-visibility-timeout.html
#
##
__author__ = "Vas Vasiliadis <vas@uchicago.edu>"

import boto3
import json
import os
import sys
import time
from typing import Any, Dict

from botocore.exceptions import ClientError, BotoCoreError, EndpointConnectionError

sys.path.insert(0, os.path.realpath(os.path.join(os.path.dirname(__file__), "..")))
import helpers

from configparser import ConfigParser, ExtendedInterpolation

config = ConfigParser(os.environ, interpolation=ExtendedInterpolation())
config.read(os.path.join(os.path.dirname(__file__), "..", "util_config.ini"))
config.read(os.path.join(os.path.dirname(__file__), "thaw_script_config.ini"))

"""
A16
Initiate thawing of archived objects from Glacier
"""


AWS_REGION = config.get("aws", "AwsRegionName", fallback="us-east-1").strip()

THAW_QUEUE_URL = config.get("sqs", "ThawQueueUrl", fallback="").strip()
WAIT_TIME = int(config.get("sqs", "WaitTime", fallback="20"))
MAX_MESSAGES = int(config.get("sqs", "MaxMessages", fallback="5"))

DDB_TABLE_NAME = config.get("dynamodb", "AnnotationsTable", fallback="").strip()
GLACIER_VAULT = config.get("glacier", "VaultName", fallback="ucmpcs").strip()


def parse_message_body(body: str) -> Dict[str, Any]:
    outer = json.loads(body)
    if isinstance(outer, dict) and outer.get("Type") == "Notification" and "Message" in outer:
        inner = outer["Message"]
        return json.loads(inner) if isinstance(inner, str) else inner
    return outer


def initiate_glacier_retrieval(glacier_client, archive_id: str, description: str) -> Dict[str, str]:
    # 1) Try Expedited
    try:
        resp = glacier_client.initiate_job(
            vaultName=GLACIER_VAULT,
            jobParameters={
                "Type": "archive-retrieval",
                "ArchiveId": archive_id,
                "Description": description,
                "Tier": "Expedited",
            },
        )
        job_id = resp.get("jobId")
        if not job_id:
            raise RuntimeError("Glacier initiate_job returned no jobId (Expedited)")
        return {"job_id": job_id, "tier": "Expedited"}

    except ClientError as e:
        # Glacier commonly returns InsufficientCapacityException for Expedited
        code = e.response.get("Error", {}).get("Code", "")

        # Checking specific error and if caught, moving to standard execution
        if code == "InsufficientCapacityException":
            print(
                f"[thaw] Expedited retrieval failed (InsufficientCapacityException). "
                f"Falling back to Standard...",
                flush=True,
            )

            # 2) Retry Standard
            resp2 = glacier_client.initiate_job(
                vaultName=GLACIER_VAULT,
                jobParameters={
                    "Type": "archive-retrieval",
                    "ArchiveId": archive_id,
                    "Description": description,
                    "Tier": "Standard",
                },
            )
            job_id2 = resp2.get("jobId")
            if not job_id2:
                raise RuntimeError("Glacier initiate_job returned no jobId (Standard)")
            return {"job_id": job_id2, "tier": "Standard"}

        raise


def handle_thaw_queue(sqs_client, ddb_table, glacier_client) -> None:
    if not THAW_QUEUE_URL:
        raise RuntimeError("Missing [sqs] ThawQueueUrl in thaw_script_config.ini")
    if not DDB_TABLE_NAME:
        raise RuntimeError("Missing [dynamodb] AnnotationsTable in thaw_script_config.ini")

    # Long poll for messages
    try:
        resp = sqs_client.receive_message(
            QueueUrl=THAW_QUEUE_URL,
            MaxNumberOfMessages=min(max(MAX_MESSAGES, 1), 10),
            WaitTimeSeconds=min(max(WAIT_TIME, 0), 20),
        )
    except EndpointConnectionError as e:
        print(f"[thaw] SQS network error: {e}", flush=True)
        return
    except (ClientError, BotoCoreError) as e:
        print(f"[thaw] SQS receive_message error: {e}", flush=True)
        return

    messages = resp.get("Messages", [])
    if not messages:
        return

    for m in messages:
        receipt = m.get("ReceiptHandle")
        body = m.get("Body", "")

        if not receipt:
            print("[thaw] Missing ReceiptHandle; skipping message", flush=True)
            continue

        try:
            payload = parse_message_body(body)
            job_id = payload.get("job_id")
            user_id = payload.get("user_id")

            if not job_id or not user_id:
                raise ValueError("Missing required fields job_id/user_id")

            print(f"[thaw] Received thaw request job_id={job_id} user_id={user_id}", flush=True)

            # Load job item
            job_resp = ddb_table.get_item(Key={"job_id": job_id})
            job = job_resp.get("Item")
            if not job:
                raise ValueError(f"Job not found in DynamoDB job_id={job_id}")

            archive_id = job.get("results_file_archive_id")
            if not archive_id:
                # Nothing to thaw; delete message
                print(f"[thaw] job_id={job_id} has no results_file_archive_id; deleting message", flush=True)
                sqs_client.delete_message(QueueUrl=THAW_QUEUE_URL, ReceiptHandle=receipt)
                continue

            # If already requested, no-op
            if job.get("results_file_restore_job_id"):
                print(f"[thaw] job_id={job_id} restore already requested; deleting message", flush=True)
                sqs_client.delete_message(QueueUrl=THAW_QUEUE_URL, ReceiptHandle=receipt)
                continue

            try:
                profile = helpers.get_user_profile(user_id)
                role = (profile.get("role") or "").lower()
                if "premium" not in role:
                    print(f"[thaw] user_id={user_id} not premium yet; leaving message in queue", flush=True)
                    continue
            except Exception as e:
                print(f"[thaw] Could not fetch profile (best effort); err={e}", flush=True)

            # Initiate retrieval with graceful degradation
            desc = f"GAS thaw results job_id={job_id}"
            res = initiate_glacier_retrieval(glacier_client, archive_id, desc)
            glacier_job_id = res["job_id"]
            tier = res["tier"]

            # Update DynamoDB for UI to show "being restored"
            ddb_table.update_item(
                Key={"job_id": job_id},
                UpdateExpression=(
                    "SET results_file_restore_job_id = :jid, "
                    "results_file_restore_status = :st, "
                    "results_file_restore_tier = :tier, "
                    "results_file_restore_requested_time = :ts"
                ),
                ExpressionAttributeValues={
                    ":jid": glacier_job_id,
                    ":st": "IN_PROGRESS",
                    ":tier": tier,
                    ":ts": int(time.time()),
                },
            )

            print(
                f"[thaw] Started Glacier retrieval job_id={job_id} "
                f"archive_id={archive_id} glacier_job_id={glacier_job_id} tier={tier}",
                flush=True,
            )

            # Delete SQS message
            sqs_client.delete_message(QueueUrl=THAW_QUEUE_URL, ReceiptHandle=receipt)
            print(f"[thaw] Deleted thaw request message for job_id={job_id}", flush=True)

        except json.JSONDecodeError as e:
            print(f"[thaw] Bad JSON; leaving message in queue: {e}", flush=True)
        except ValueError as e:
            print(f"[thaw] Invalid payload/state; leaving message in queue: {e}", flush=True)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            print(f"[thaw] AWS error ({code}); leaving message in queue: {e}", flush=True)
        except Exception as e:
            print(f"[thaw] Unexpected error; leaving message in queue: {e}", flush=True)


def main() -> None:
    print(f"[thaw] Starting. Queue={THAW_QUEUE_URL} Vault={GLACIER_VAULT}", flush=True)

    sqs_client = boto3.client("sqs", region_name=AWS_REGION)
    ddb = boto3.resource("dynamodb", region_name=AWS_REGION)
    ddb_table = ddb.Table(DDB_TABLE_NAME)
    glacier_client = boto3.client("glacier", region_name=AWS_REGION)

    while True:
        handle_thaw_queue(sqs_client, ddb_table, glacier_client)


if __name__ == "__main__":
    main()