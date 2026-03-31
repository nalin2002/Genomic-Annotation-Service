# run.py
#
# Copyright (C) 2011-2025 Vas Vasiliadis
# University of Chicago
#
# Wrapper script for running AnnTools
#
##
# References:
# 1) https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/upload_file.html
# 2) https://docs.python.org/3/library/pathlib.html#pathlib.Path.unlink
# 3) https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
# 4) https://realpython.com/python-boto3-aws-s3/
# 5) https://github.com/aws-samples/aws-dynamodb-examples/tree/master/examples/SDK/python/data_plane/WorkingWithItems
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import json
import sys
import time
import driver
import os
from pathlib import Path
import boto3
from botocore.exceptions import ClientError
import configparser

from botocore.exceptions import ClientError


def load_config() -> configparser.ConfigParser:
    base_dir = Path(__file__).resolve().parent
    cfg_path = base_dir / "annotator_config.ini"

    print(f"[run.py] Loading config from {cfg_path}")

    cfg = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
    read_ok = cfg.read(cfg_path)
    if not read_ok:
        raise FileNotFoundError(f"Could not read config file at {cfg_path!r}")
    return cfg


def cfg_get(cfg: configparser.ConfigParser, section: str, key: str, fallback: str) -> str:
    try:
        return cfg.get(section, key, fallback=fallback)
    except (configparser.NoSectionError, configparser.NoOptionError):
        return fallback


cfg = load_config()

# AWS Configuration
CNETID =  cfg_get(cfg, "DEFAULT", "CnetId", "nalinprabhath")
AWS_REGION = cfg_get(cfg, "aws", "AwsRegionName", "us-east-1")

S3_RESULTS_BUCKET =cfg_get(cfg, "s3", "ResultsBucketName", "gas-results")
DB_TABLE = cfg_get(cfg, "DynamoDb", "AnnotationsTable", f"{CNETID}_annotations")

USERNAME =  cfg_get(cfg, "app", "Username", "userX")

# Dynamo DB configuration
db = boto3.resource("dynamodb", region_name=AWS_REGION)
jobs_table = db.Table(DB_TABLE)

# S3 client
s3_client = boto3.client("s3", region_name=AWS_REGION)

sns_client = boto3.client("sns", region_name=AWS_REGION)
JOB_RESULTS_TOPIC_ARN = cfg_get(cfg, "sns", "JobResultsTopicArn","")

"""A rudimentary timer for coarse-grained profiling
"""
class Timer(object):
  def __init__(self, verbose=True):
    self.verbose = verbose

  def __enter__(self):
    self.start = time.time()
    return self

  def __exit__(self, *args):
    self.end = time.time()
    self.secs = self.end - self.start
    if self.verbose:
      print(f"Approximate runtime: {self.secs:.2f} seconds")

# Function to upload S3 file
def upload_to_s3(local_file_path: Path,bucket: str, s3_key: str):
    try:
        s3_client.upload_file(str(local_file_path), bucket, s3_key)
        return True
    except ClientError as e:
        print(f"ERROR: Failed to upload to S3")
        return False
    except Exception as e:
        print(f"ERROR: Exception raised error uploading to S3")
        return False

def db_job_update( job_id: str,
                      results_bucket: str,
                      result_key: str,
                      log_key: str,
                      complete_time: int) -> bool:
    try:
        jobs_table.update_item(
            Key={"job_id": job_id},
            UpdateExpression=(
                "SET s3_results_bucket = :rb, "
                "s3_key_result_file = :rk, "
                "s3_key_log_file = :lk, "
                "complete_time = :ct, "
                "job_status = :st"
            ),
            ExpressionAttributeValues={
                ":rb": results_bucket,
                ":rk": result_key,
                ":lk": log_key,
                ":ct": complete_time,
                ":st": "COMPLETED",
            },
        )
        return True
    except ClientError as e:
        print("ERROR: DynamoDB ClientError:", e.response["Error"]["Message"])
        return False
    except Exception as e:
        print(f"ERROR: Failed to update DynamoDB for job_id={job_id}: {e}")
        return False

def delete_local_file(file_path: Path) -> None:
    try:
        if file_path.exists():
            file_path.unlink()
    except Exception as e:
        print(f"Failed to delete {file_path}: {e}")

def publish_sns_notif(topic_arn: str, payload: dict) -> bool:
    if not topic_arn:
        print(f"ERROR: SNS topic ARN not set: {topic_arn}")
        return False
    try:
        sns_client.publish(
            TopicArn=topic_arn,
            Message=json.dumps(payload),
            Subject= f"Job Completed : {payload.get('job_id','')}",
        )
        return True
    except ClientError as e:
        print(f"ERROR: Failed to publish SNS notification: {e}")
        return False
    except Exception as e:
        print(f"ERROR: Failed to publish SNS notification: {e}")
        return False

if __name__ == '__main__':
    if len(sys.argv) <= 1:
        print("ERROR: A valid .vcf file must be provided as input")
        sys.exit(1)

    cfg = load_config()

    aws_region = cfg["aws"].get("AwsRegionName", "us-east-1")
    results_bucket = cfg["s3"].get("ResultsBucketName")
    key_prefix = cfg["s3"].get("KeyPrefix")
    annotations_table = cfg["DynamoDb"].get("AnnotationsTable")
    file_type = "vcf"

    input_file_path = Path(sys.argv[1])

    if not input_file_path.exists():
        print(f"ERROR: Input file not found: {input_file_path}")
        sys.exit(1)

    input_file_name = input_file_path.name

    # Extracting job_id from the parent directory
    job_id = input_file_path.parent.name
    user_uuid = input_file_path.parent.parent.name

    # Run the annotation
    with Timer():
        driver.run(str(input_file_path), 'vcf')

    results_file = input_file_path.with_suffix(".annot.vcf")
    log_file = input_file_path.with_suffix(".vcf.count.log")

    # S3 keys
    s3_prefix = f"{key_prefix}{user_uuid}/{job_id}/"
    result_key = f"{s3_prefix}{results_file.name}"
    log_key = f"{s3_prefix}{log_file.name}"

    ok = True

    if results_file.exists():
        ok = ok and upload_to_s3(results_file, results_bucket, result_key)
    else:
        print(f"ERROR: Results file missing: {results_file}")
        ok = False

    if log_file.exists():
        ok = ok and upload_to_s3( log_file, results_bucket, log_key)
    else:
        print(f"ERROR: Log file missing: {log_file}")
        ok = False

    if not ok:
        print("ERROR: Upload failed; not marking COMPLETED.")
        sys.exit(1)

    # Update DynamoDB as COMPLETED
    complete_time = int(time.time())
    if not db_job_update(job_id, S3_RESULTS_BUCKET, result_key, log_key, complete_time):
        print("ERROR: DynamoDB update failed.")
        sys.exit(1)

    notification_payload = {
        "job_id": job_id,
        "user_id": user_uuid,
        "complete_time": complete_time,
        "s3_results_bucket": S3_RESULTS_BUCKET,
        "s3_key_result_file": result_key,
        "s3_key_log_file": log_key,
    }
    published = publish_sns_notif(JOB_RESULTS_TOPIC_ARN, notification_payload)
    print(notification_payload, flush=True)

    if published:
        print(f" Published results notification for job_id = {job_id}",flush=True)
    else:
        print(f" Failed to publish results notification for job_id = {job_id}",flush=True)

    #Clean up local job files
    delete_local_file(input_file_path)
    delete_local_file(results_file)
    delete_local_file(log_file)

    sys.exit(0)


