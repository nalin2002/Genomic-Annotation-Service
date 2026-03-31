# archive_script.py
#
# Archive free user data (results file only)
#
# Copyright (C) 2015-2023 Vas Vasiliadis
# University of Chicago
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/upload_archive.html
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/get_object.html
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/delete_object.html
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/receive_message.html
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/delete_message.html
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/get_item.html
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/update_item.html
##
__author__ = "Vas Vasiliadis <vas@uchicago.edu>"

import boto3
import os
import sys
import json
from typing import Any, Dict, Optional
from pathlib import Path
from botocore.exceptions import ClientError, BotoCoreError, EndpointConnectionError

# Importing utility helpers
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))
import helpers

from configparser import ConfigParser, ExtendedInterpolation

config = ConfigParser(os.environ, interpolation=ExtendedInterpolation())
config.read("../util_config.ini")
config.read("archive_script_config.ini")

"""A14
Archive free user results files
"""

AWS_REGION = config.get("aws", "AwsRegionName", fallback="us-east-1").strip()

ARCHIVE_QUEUE_URL = config.get("sqs", "ArchiveQueueUrl", fallback="").strip()
WAIT_TIME = int(config.get("sqs", "WaitTime", fallback="20"))
MAX_MESSAGES = int(config.get("sqs", "MaxMessages", fallback="5"))

DDB_TABLE_NAME = config.get("dynamodb", "AnnotationsTable", fallback="").strip()
RESULTS_BUCKET_DEFAULT = config.get("s3", "ResultsBucketName", fallback="gas-results").strip()

GLACIER_VAULT = config.get("glacier", "VaultName", fallback="ucmpcs").strip()


def parse_message_body(body: str) -> Dict[str, Any]:
    outer = json.loads(body)
    if isinstance(outer, dict) and outer.get("Type") == "Notification" and "Message" in outer:
        inner = outer["Message"]
        return json.loads(inner) if isinstance(inner, str) else inner
    return outer


def handle_archive_queue(
    sqs_client=None,
    ddb_table=None,
    s3_client=None,
    glacier_client=None,
):
    if not ARCHIVE_QUEUE_URL:
        raise RuntimeError("Missing [sqs] ArchiveQueueUrl in archive_script_config.ini")
    if not DDB_TABLE_NAME:
        raise RuntimeError("Missing [dynamodb] AnnotationsTable in archive_script_config.ini")

    if sqs_client is None:
        sqs_client = boto3.client("sqs", region_name=AWS_REGION)
    if s3_client is None:
        s3_client = boto3.client("s3", region_name=AWS_REGION)
    if glacier_client is None:
        glacier_client = boto3.client("glacier", region_name=AWS_REGION)
    if ddb_table is None:
        ddb = boto3.resource("dynamodb", region_name=AWS_REGION)
        ddb_table = ddb.Table(DDB_TABLE_NAME)

    # Reading messages from archive queue
    try:
        resp = sqs_client.receive_message(
            QueueUrl=ARCHIVE_QUEUE_URL,
            MaxNumberOfMessages=min(max(MAX_MESSAGES, 1), 10),
            WaitTimeSeconds=min(max(WAIT_TIME, 0), 20),
        )
    except (EndpointConnectionError,) as e:
        print(f"[archive] SQS network error: {e}", flush=True)
        return
    except (ClientError, BotoCoreError) as e:
        print(f"[archive] SQS receive_message error: {e}", flush=True)
        return

    msgs = resp.get("Messages", [])
    if not msgs:
        return

    for m in msgs:
        receipt = m.get("ReceiptHandle")
        body = m.get("Body", "")

        if not receipt:
            print("[archive] Missing ReceiptHandle; skipping", flush=True)
            continue

        try:
            payload = parse_message_body(body)

            job_id = payload.get("job_id")
            user_id = payload.get("user_id")

            if not job_id or not user_id:
                raise ValueError("Missing required fields job_id/user_id")

            print(f"[archive] Received archive request job_id={job_id}", flush=True)

            # Fetching job item
            job_resp = ddb_table.get_item(Key={"job_id": job_id})
            job = job_resp.get("Item")
            if not job:
                raise ValueError(f"Job not found in DynamoDB job_id={job_id}")

            # If already archived, just delete message
            if job.get("results_file_archive_id"):
                print(f"[archive] job_id={job_id} already archived; deleting SQS message", flush=True)
                sqs_client.delete_message(QueueUrl=ARCHIVE_QUEUE_URL, ReceiptHandle=receipt)
                continue

            # Checking user role from accounts DB
            profile = helpers.get_user_profile(user_id)
            role = (profile.get("role") or "").lower()

            # Normalize role strings you use
            is_premium = "premium" in role

            if is_premium:
                print(f"[archive] job_id={job_id} user is PREMIUM; skipping archival", flush=True)
                sqs_client.delete_message(QueueUrl=ARCHIVE_QUEUE_URL, ReceiptHandle=receipt)
                continue

            # Free user-  archive results file only
            bucket = job.get("s3_results_bucket") or payload.get("s3_results_bucket")
            key = job.get("s3_key_result_file") or payload.get("s3_key_result_file")
            if not bucket or not key:
                raise ValueError(f"Missing results bucket/key for job_id={job_id}")

            # Download results bytes from S3
            try:
                obj = s3_client.get_object(Bucket=bucket, Key=key)
                data = obj["Body"].read()
            except ClientError as e:
                code = e.response.get("Error", {}).get("Code", "")
                if code in ("NoSuchKey", "404"):
                    raise ValueError(f"Results object not found in S3 for job_id={job_id}")
                raise

            # Uploading to Glacier vault 'ucmpcs'
            try:
                up = glacier_client.upload_archive(
                    vaultName=GLACIER_VAULT,
                    body=data,
                    archiveDescription=f"gas-results:{bucket}/{key}",
                )
                archive_id = up.get("archiveId")
                if not archive_id:
                    raise ValueError("Glacier upload returned no archiveId")
            except (ClientError, BotoCoreError) as e:
                raise RuntimeError(f"Glacier upload failed: {e}")


            # 6) Updating DynamoDB with archive ID
            ddb_table.update_item(
                Key={"job_id": job_id},
                UpdateExpression="SET results_file_archive_id = :aid",
                ExpressionAttributeValues={":aid": archive_id},
            )

            # 7) Deleting results file from S3
            s3_client.delete_object(Bucket=bucket, Key=key)

            print(
                f"[archive] Archived job_id={job_id} results to Glacier archiveId={archive_id} and deleted s3://{bucket}/{key}",
                flush=True,
            )

            # 8) Deleting SQS message
            sqs_client.delete_message(QueueUrl=ARCHIVE_QUEUE_URL, ReceiptHandle=receipt)
            print(f"[archive] Deleted archive request message for job_id={job_id}", flush=True)

        except json.JSONDecodeError as e:
            print(f"[archive] Bad JSON; leaving message in queue: {e}", flush=True)
        except ValueError as e:
            print(f"[archive] Invalid payload/state; leaving message in queue: {e}", flush=True)
        except (ClientError, BotoCoreError) as e:
            print(f"[archive] AWS error; leaving message in queue: {e}", flush=True)
        except Exception as e:
            print(f"[archive] Unexpected error; leaving message in queue: {e}", flush=True)


def main():
    print(f"[archive] Starting. Queue={ARCHIVE_QUEUE_URL} Vault={GLACIER_VAULT}", flush=True)

    sqs_client = boto3.client("sqs", region_name=AWS_REGION)
    s3_client = boto3.client("s3", region_name=AWS_REGION)
    glacier_client = boto3.client("glacier", region_name=AWS_REGION)
    ddb = boto3.resource("dynamodb", region_name=AWS_REGION)
    ddb_table = ddb.Table(DDB_TABLE_NAME)

    while True:
        handle_archive_queue(
            sqs_client=sqs_client,
            ddb_table=ddb_table,
            s3_client=s3_client,
            glacier_client=glacier_client,
        )


if __name__ == "__main__":
    main()

### EOF