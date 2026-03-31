"""
References:
1) https://flask.palletsprojects.com/en/latest/quickstart/#routing
2) https://realpython.com/python-subprocess/
3) https://stackoverflow.com/questions/568271/how-to-check-if-there-exists-a-process-with-a-given-pid-in-python
4) https://flask.palletsprojects.com/en/latest/
5) https://12factor.net/config
6) https://github.com/aws-samples/aws-dynamodb-examples/tree/master/examples/SDK/python/data_plane/WorkingWithItems
7)  https://realpython.com/python-boto3-aws-s3/
8) https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/receive_message.html
9) https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/delete_message.html
10) https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html
11) https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs-example-visibility-timeout.html
"""

import json
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import ClientError, EndpointConnectionError, BotoCoreError
import configparser

HOME = Path.home()

# Config loading
def load_config() -> configparser.ConfigParser:
    cfg_path = "annotator_config.ini"
    cfg = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
    read_ok = cfg.read(cfg_path)
    if not read_ok:
        raise FileNotFoundError(f"Could not read config file at {cfg_path!r}")
    return cfg

def cfg_get(cfg: configparser.ConfigParser, section: str, key: str, fallback: Optional[str] = None) -> Optional[str]:
    try:
        return cfg.get(section, key, fallback=fallback)
    except (configparser.NoSectionError, configparser.NoOptionError):
        return fallback

def cfg_get_int(cfg: configparser.ConfigParser, section: str, key: str, fallback: int) -> int:
    val = cfg_get(cfg, section, key, None)
    if val is None or str(val).strip() == "":
        return fallback
    try:
        return int(val)
    except ValueError:
        return fallback

cfg = load_config()

CNETID = cfg_get(cfg, "DEFAULT", "CnetId", "nalinprabhath")
AWS_REGION = cfg_get(cfg, "aws", "AwsRegionName", "us-east-1")

DB_TABLE = cfg_get(cfg, "gas", "AnnotationsTable", f"{CNETID}_annotations")

S3_INPUTS_BUCKET = cfg_get(cfg, "s3", "InputsBucketName", "gas-inputs")
S3_KEY_PREFIX =  cfg_get(cfg, "s3", "KeyPrefix", f"{CNETID}/")

SQS_QUEUE_URL = cfg_get(cfg, "sqs", "SqsQueueUrl", "")
MAX_MESSAGES = int(cfg_get_int(cfg, "sqs", "MaxMessages", 10))
WAIT_TIME = int(cfg_get_int(cfg, "sqs", "WaitTime", 20))
VISIBILITY_TIMEOUT = cfg_get(cfg, "sqs", "VisibilityTimeout", None)

BASE_DIR = Path(__file__).resolve().parent

ANNTOOLS_DIR = BASE_DIR
JOBS_DIR = BASE_DIR / "jobs"
DATA_DIR = BASE_DIR / "data"

if not SQS_QUEUE_URL:
    raise ValueError("Missing SQS queue URL. Set SQS_QUEUE_URL in annotator_config.ini")

db = boto3.resource("dynamodb", region_name=AWS_REGION)
jobs_table = db.Table(DB_TABLE)
s3_client = boto3.client("s3", region_name=AWS_REGION)
sqs_client = boto3.client("sqs", region_name=AWS_REGION)

# Helpers
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
def log(msg: str) -> None:
    print(f"[a9_annotator] {msg}", file=sys.stderr, flush=True)
def ensure_jobs_dir() -> None:
    JOBS_DIR.mkdir(parents=True, exist_ok=True)
def job_dir(user_uuid: str, job_id: str) -> Path:
    return JOBS_DIR / user_uuid / job_id
def meta_path(user_uuid: str, job_id: str) -> Path:
    return job_dir(user_uuid, job_id) / "meta.json"
def pid_path(user_uuid: str, job_id: str) -> Path:
    return job_dir(user_uuid, job_id) / "pid.txt"
def runner_log_path(user_uuid: str, job_id: str) -> Path:
    return job_dir(user_uuid, job_id) / "runner.log"


@dataclass
class JobMeta:
    job_id: str
    input_file: str
    created_at: str
    pid: Optional[int] = None

    def save(self, path: Path) -> None:
        path.write_text(
            json.dumps(
                {
                    "job_id": self.job_id,
                    "input_file": self.input_file,
                    "created_at": self.created_at,
                    "pid": self.pid,
                },
                indent=2,
            ),
            encoding="utf-8",
        )

# Function to get the message body from the SQS request message
def parse_job_from_message_body(message_body: str) -> Dict[str, Any]:
    outer = json.loads(message_body)

    if isinstance(outer, dict) and outer.get("Type") == "Notification" and "Message" in outer:
        inner = outer["Message"]
        if isinstance(inner, str):
            return json.loads(inner)
        if isinstance(inner, dict):
            return inner

    if isinstance(outer, dict):
        return outer

    raise ValueError("Expected JSON object.")

# Function to change the status from pending to running
def db_pending_to_running(job_id: str) -> None:
    jobs_table.update_item(
        Key={"job_id": job_id},
        UpdateExpression="SET job_status = :r",
        ConditionExpression="job_status = :p",
        ExpressionAttributeValues={":r": "RUNNING", ":p": "PENDING"},
    )

def download_input(bucket: str, key: str, dest: Path) -> None:
    s3_client.download_file(bucket, key, str(dest))

def receive_messages() -> Dict[str, Any]:
    kwargs: Dict[str, Any] = {
        "QueueUrl": SQS_QUEUE_URL,
        "MaxNumberOfMessages": min(max(MAX_MESSAGES, 1), 10),
        "WaitTimeSeconds": min(max(WAIT_TIME, 0), 20),
    }
    if VISIBILITY_TIMEOUT:
        try:
            kwargs["VisibilityTimeout"] = int(VISIBILITY_TIMEOUT)
        except ValueError:
            log(f"Ignoring invalid SQS_VISIBILITY_TIMEOUT={VISIBILITY_TIMEOUT!r}")

    return sqs_client.receive_message(**kwargs)


def delete_message(receipt_handle: str) -> None:
    sqs_client.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)


def ann_job(user_uuid: str, job_id: str, staged_input: Path, cwd: Path) -> int:
    BASE_DIR = Path(__file__).resolve().parent
   # cmd = ["python3", str(BASE_DIR / "run.py"), str(staged_input)]
    cmd = [sys.executable, str(BASE_DIR / "run.py"), str(staged_input)]
    with runner_log_path(user_uuid, job_id).open("ab") as rlog:
        proc = subprocess.Popen(cmd, cwd=str(cwd), stdout=rlog, stderr=subprocess.STDOUT)
    return proc.pid


def handle_one_message(msg: Dict[str, Any]) -> None:
    receipt_handle = msg.get("ReceiptHandle")
    body = msg.get("Body", "")

    if not receipt_handle:
        raise ValueError("Missing ReceiptHandle in SQS message")

    job = parse_job_from_message_body(body)

    job_id = job.get("job_id")
    bucket = job.get("s3_inputs_bucket")
    key = job.get("s3_key_input_file")

    if not job_id or not bucket or not key:
        raise ValueError("Missing required fields: job_id, s3_inputs_bucket, s3_key_input_file")

    filename = key.split("~$", 1)[-1] if "~$" in key else key.split("/")[-1]

    user_uuid = job.get("user_id")
    if not user_uuid:
        raise ValueError("Missing user_id in job message")

    jdir = JOBS_DIR / user_uuid / job_id
    jdir.mkdir(parents=True, exist_ok=True)
    staged_input = jdir / filename

    # Checking if process ID already exists for the raised job
    if pid_path(user_uuid, job_id).exists():
        log(f"Job {job_id} already has pid.txt; deleting message as duplicate.")
        delete_message(receipt_handle)
        return

    if not staged_input.exists():
        download_input(bucket, key, staged_input)

    meta = JobMeta(job_id=job_id, input_file=filename, created_at=now_iso(), pid=None)
    meta.save(meta_path(user_uuid,job_id))

    # Updating DB status
    try:
        db_pending_to_running(job_id)
    except Exception as e:
        log(f"Failed to change status in DB for job_id={job_id}: {e}")
        raise

    # Triggering sub process
    try:
        pid = ann_job(user_uuid, job_id, staged_input, jdir)  # must raise if it fails
    except Exception as e:
        log(f"Failed to spawn subprocess for job_id={job_id}; leaving message in queue: {e}")
        raise

    pid_path(user_uuid,job_id).write_text(str(pid), encoding="utf-8")
    meta.pid = pid
    meta.save(meta_path(user_uuid,job_id))

    # Delete after a successful trigger
    delete_message(receipt_handle)
    log(f"Spawned job_id={job_id} pid={pid} and deleted SQS message")


def main() -> int:
    try:
        ensure_jobs_dir()
    except OSError as e:
        log(f"Filesystem error creating jobs directory: {e}")
        return 2

    while True:
        try:
            resp = receive_messages()
        except EndpointConnectionError as e:
            log(f"Failure to receive messages (network): {e}")
            continue
        except ClientError as e:
            log(f"Failure to receive messages (ClientError): {e}")
            continue
        except BotoCoreError as e:
            log(f"Failure to receive messages (BotoCoreError): {e}")
            continue

        messages = resp.get("Messages", [])
        if not messages:
            continue

        for m in messages:
            try:
                handle_one_message(m)
            except json.JSONDecodeError as e:
                log(f"Improper JSON format in message body; leaving message in queue: {e}")
            except ClientError as e:
                log(f"AWS ClientError while handling message; leaving message in queue: {e}")
            except EndpointConnectionError as e:
                log(f"Network error while handling message; leaving message in queue: {e}")
            except FileExistsError as e:
                log(f"{e}; leaving message in queue")
            except ValueError as e:
                log(f"Invalid message payload; leaving message in queue: {e}")

if __name__ == "__main__":
    raise SystemExit(main())