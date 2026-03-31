import boto3
import json
import os
import re
import time
from botocore.exceptions import ClientError
# https://docs.aws.amazon.com/lambda/latest/dg/python-handler.html
#https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/get_job_output.html
# https://docs.aws.amazon.com/boto3/latest/reference/services/glacier/client/delete_archive.html
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object.html

# ---- hardcode allowed for Lambda (per assignment) ----
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
DYNAMODB_TABLE = "nalinprabhath_annotations"
RESULTS_BUCKET = "gas-results"
GLACIER_VAULT = "ucmpcs"

dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
jobs_table = dynamodb.Table(DYNAMODB_TABLE)

glacier = boto3.client("glacier", region_name=AWS_REGION)
s3 = boto3.client("s3", region_name=AWS_REGION)


def parse_sns_event(event: dict) -> dict:
    rec = event["Records"][0]
    msg = rec["Sns"]["Message"]
    return json.loads(msg)


def extract_job_info_from_description(desc: str):
    if not desc:
        return None, None

    m_job = re.search(r"job_id=([0-9a-fA-F-]{36})", desc)
    m_key = re.search(r"s3_key=([^|]+)", desc)

    job_id = m_job.group(1) if m_job else None
    s3_key = m_key.group(1) if m_key else None
    return job_id, s3_key


def lambda_handler(event, context):
    try:
        glacier_msg = parse_sns_event(event)
    except Exception as e:
        print(f"[restore] Failed parsing SNS event: {e}")
        return {"ok": False, "error": "bad_event"}

    glacier_job_id = glacier_msg.get("JobId")
    archive_id = glacier_msg.get("ArchiveId")
    desc = glacier_msg.get("JobDescription")

    print(f"[restore] Received Glacier completion msg glacier_job_id={glacier_job_id}")

    job_id, s3_key = extract_job_info_from_description(desc)

    if not job_id:
        print(f"[restore] Could not extract job_id from description: {desc}")
        return {"ok": False, "error": "missing_job_id"}

    # Fallback: if old description format does not contain s3_key,
    # pull required info from DynamoDB using job_id
    if not s3_key or not archive_id:
        try:
            resp = jobs_table.get_item(Key={"job_id": job_id})
            job = resp.get("Item")
            if not job:
                print(f"[restore] DynamoDB item not found for job_id={job_id}")
                return {"ok": False, "error": "ddb_missing"}

            if not s3_key:
                s3_key = job.get("s3_key_result_file")

            if not archive_id:
                archive_id = job.get("results_file_archive_id")

        except ClientError as e:
            print(f"[restore] DynamoDB get_item failed: {e}")
            return {"ok": False, "error": "ddb_get_failed"}

    if not s3_key:
        print(f"[restore] Missing s3_key for job_id={job_id}")
        return {"ok": False, "error": "missing_s3_key"}

    if not archive_id:
        print(f"[restore] Missing archive_id for job_id={job_id}")
        return {"ok": False, "error": "missing_archive_id"}

    # Fetch bytes from Glacier job output
    try:
        out = glacier.get_job_output(vaultName=GLACIER_VAULT, jobId=glacier_job_id)
        stream = out.get("body") or out.get("Body")
        if stream is None:
            print(f"[restore] get_job_output returned no body: {out}")
            return {"ok": False, "error": "missing_body"}

        data = stream.read()
        print(f"[restore] Downloaded {len(data)} bytes from Glacier job output")
    except ClientError as e:
        print(f"[restore] get_job_output failed: {e}")
        return {"ok": False, "error": "glacier_get_output_failed"}

    # Put back into S3 results bucket
    try:
        s3.put_object(Bucket=RESULTS_BUCKET, Key=s3_key, Body=data)
        print(f"[restore] Restored to s3://{RESULTS_BUCKET}/{s3_key}")
    except ClientError as e:
        print(f"[restore] S3 put_object failed: {e}")
        return {"ok": False, "error": "s3_put_failed"}

    # Delete Glacier archive
    try:
        glacier.delete_archive(vaultName=GLACIER_VAULT, archiveId=archive_id)
        print(f"[restore] Deleted Glacier archive archiveId={archive_id}")
    except ClientError as e:
        print(f"[restore] WARNING: delete_archive failed: {e}")

    # Update DynamoDB state
    try:
        jobs_table.update_item(
            Key={"job_id": job_id},
            UpdateExpression=(
                "SET results_file_restore_status = :done, "
                "results_file_restored_time = :ts "
                "REMOVE results_file_archive_id, results_file_restore_job_id"
            ),
            ExpressionAttributeValues={
                ":done": "COMPLETED",
                ":ts": int(time.time()),
            },
        )
        print(f"[restore] Updated DynamoDB restore status for job_id={job_id}")
    except ClientError as e:
        print(f"[restore] WARNING: DynamoDB update_item failed: {e}")

    return {"ok": True, "job_id": job_id, "s3_key": s3_key}