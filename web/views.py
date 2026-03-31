# views.py
#
# Copyright (C) 2015-2023 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
# https://flask.palletsprojects.com/en/latest/api/#flask.render_template
# https://stackoverflow.com/questions/31184187/how-to-upload-file-to-amazon-s3-using-a-pre-signed-url
# https://flask.palletsprojects.com/en/latest/api/#flask.Request
# https://flask.palletsprojects.com/en/latest/api/#flask.render_template
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/generate_presigned_post.html
# https://boto3.amazonaws.com/v1/documentation/api/latest/guide/dynamodb.html
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/index.html
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/update_item.html
# https://requests.readthedocs.io/en/latest/user/quickstart/#errors-and-exceptions
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/publish.html
# https://docs.stripe.com/api/subscriptions/create
# https://stripe.com/docs/api/customers/create
# https://docs.aws.amazon.com/cli/latest/reference/glacier/initiate-job.html
# https://docs.aws.amazon.com/boto3/latest/reference/services/glacier/client/initiate_job.html

##
__author__ = "Vas Vasiliadis <vas@uchicago.edu>"

import uuid
import time
import json
from datetime import datetime

import boto3
from botocore.client import Config
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError, BotoCoreError

from flask import abort, redirect, render_template, request, session, url_for, jsonify
from app import app
from decorators import authenticated, is_premium

dynamodb = boto3.resource("dynamodb", region_name=app.config["AWS_REGION_NAME"])
jobs_table = dynamodb.Table(app.config["AWS_DYNAMO_DB"])

sns_client = boto3.client("sns", region_name=app.config["AWS_REGION_NAME"])
s3 = boto3.client("s3", region_name=app.config["AWS_REGION_NAME"])

CNETID = app.config["CNETID"]
sqs = boto3.client("sqs", region_name=app.config["AWS_REGION_NAME"])


def success(http_code: int, data: dict):
    return jsonify({"code": http_code, "status": "success", "data": data}), http_code


def error(http_code: int, message: str):
    return jsonify({"code": http_code, "status": "error", "message": message}), http_code


def current_user_id() -> str:
    uid = session.get("primary_identity")
    if not uid:
        abort(401)
    return uid


# Function to Convert epoch seconds to instance local time
def epoch_to_local_str(epoch_val) -> str:
    if epoch_val is None:
        return "—"
    try:
        return datetime.fromtimestamp(int(epoch_val)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return "—"


def get_job_or_404(job_id: str) -> dict:
    try:
        resp = jobs_table.get_item(Key={"job_id": job_id})
        job = resp.get("Item")
        if not job:
            abort(404)
        return job
    except (ClientError, BotoCoreError) as e:
        app.logger.error(f"DynamoDB get_item failed for job_id={job_id}: {e}")
        abort(500)


def ensure_job_owner(job: dict, user_id: str):
    if job.get("user_id") != user_id:
        abort(403)


@app.route("/annotate", methods=["GET"])
@authenticated
def annotate():
    unique_id = str(uuid.uuid4())
    bucket_name = app.config["AWS_S3_INPUTS_BUCKET"]
    user_id = session["primary_identity"]

    s3_key_prefix = f"{CNETID}/{user_id}/"
    object_key = f"{s3_key_prefix}{unique_id}~${{filename}}"

    s3_sigv4 = boto3.client(
        "s3",
        region_name=app.config["AWS_REGION_NAME"],
        config=Config(signature_version="s3v4"),
    )

    redirect_url = str(request.url) + "/job"

    encryption = app.config["AWS_S3_ENCRYPTION"]
    acl = app.config["AWS_S3_ACL"]

    fields = {
        "success_action_redirect": redirect_url,
        "x-amz-server-side-encryption": encryption,
        "acl": acl,
        "csrf_token": app.config["SECRET_KEY"],
    }
    conditions = [
        ["starts-with", "$success_action_redirect", redirect_url],
        {"x-amz-server-side-encryption": encryption},
        {"acl": acl},
        ["starts-with", "$csrf_token", ""],
    ]

    try:
        presigned_post = s3_sigv4.generate_presigned_post(
            Bucket=bucket_name,
            Key=object_key,
            Fields=fields,
            Conditions=conditions,
            ExpiresIn=app.config["AWS_SIGNED_REQUEST_EXPIRATION"],
        )
    except ClientError as e:
        app.logger.error(f"Unable to generate presigned URL for upload: {e}")
        abort(500)

    return render_template("annotate.html", s3_post=presigned_post, role=session["role"]
                           , free_max_upload_bytes=app.config.get("FREE_MAX_UPLOAD_BYTES", 150 * 1024))


@app.route("/annotate/job", methods=["GET"])
@authenticated
def create_annotation_job_request():
    user_uuid = current_user_id()
    bucket = request.args.get("bucket")
    key = request.args.get("key")

    if not bucket or not key:
        return error(400, "Missing bucket or key in S3 redirect query parameters.")

    input_file_name = key.split("~$", 1)[-1] if "~$" in key else key.split("/")[-1]

    job_id = str(uuid.uuid4())
    submit_time = int(time.time())

    item = {
        "job_id": job_id,
        "user_id": user_uuid,
        "input_file_name": input_file_name,
        "s3_inputs_bucket": bucket,
        "s3_key_input_file": key,
        "submit_time": submit_time,
        "job_status": "PENDING",
    }

    try:
        jobs_table.put_item(Item=item)
    except (ClientError, BotoCoreError) as e:
        return error(500, f"Failed to add job to DynamoDB (AWS error): {e}")
    except Exception as e:
        return error(500, f"Failed to add job to DynamoDB: {e}")

    try:
        sns_client.publish(
            TopicArn=app.config["AWS_SNS_TOPIC_ARN"],
            Message=json.dumps(item),
            Subject="GAS Job Submitted",
        )
    except (ClientError, BotoCoreError) as e:
        return error(500, f"Failed to publish SNS notification (AWS error): {e}")
    except Exception as e:
        return error(500, f"Failed to publish SNS notification: {e}")

    return render_template("annotate_confirm.html", job_id=job_id)


@app.route("/annotations", methods=["GET"])
@authenticated
def annotations_list():
    user_id = current_user_id()

    try:
        resp = jobs_table.query(
            IndexName="user_id_index",
            KeyConditionExpression=Key("user_id").eq(user_id),
            ScanIndexForward=False,  # newest first (if your GSI has a sort key)
        )
        jobs = resp.get("Items", [])

        for j in jobs:
            j["request_time_local"] = epoch_to_local_str(j.get("submit_time"))

    except (ClientError, BotoCoreError) as e:
        app.logger.error(f"Failed to query DynamoDB for user jobs: {e}")
        abort(500)
    except Exception as e:
        app.logger.error(f"Unexpected error querying user jobs: {e}")
        abort(500)

    return render_template("annotations.html", annotations=jobs)


@app.route("/annotations/<id>", methods=["GET"])
@authenticated
def annotation_details(id):
    user_id = current_user_id()
    job = get_job_or_404(id)
    ensure_job_owner(job, user_id)

    job["request_time_local"] = epoch_to_local_str(job.get("submit_time"))

    if job.get("job_status") == "COMPLETED":
        job["complete_time_local"] = epoch_to_local_str(
            job.get("completed_time") or job.get("complete_time") or job.get("end_time")
        )

    return render_template("annotation.html", job=job)


@app.route("/annotations/<id>/input", methods=["GET"])
@authenticated
def download_input_file(id):
    user_id = current_user_id()
    job = get_job_or_404(id)
    ensure_job_owner(job, user_id)

    bucket = job.get("s3_inputs_bucket")
    key = job.get("s3_key_input_file")
    if not bucket or not key:
        abort(404)

    try:
        url = s3.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": bucket,
                "Key": key,
                "ResponseContentDisposition": (
                    f'attachment; filename="{job.get("input_file_name", "input.vcf")}"'
                ),
            },
            ExpiresIn=300,
        )
        return redirect(url)
    except (ClientError, BotoCoreError) as e:
        app.logger.error(f"Failed to presign input download for job_id={id}: {e}")
        abort(500)


@app.route("/annotations/<id>/results", methods=["GET"])
@authenticated
def download_results_file(id):
    user_id = current_user_id()
    job = get_job_or_404(id)
    ensure_job_owner(job, user_id)

    if job.get("job_status") != "COMPLETED":
        abort(403)

    if job.get("results_file_archive_id"):
        return redirect(url_for("annotation_details", id=id))

    bucket = job.get("s3_results_bucket") or app.config.get("AWS_S3_RESULTS_BUCKET")
    key = (
        job.get("s3_key_result_file")
    )

    if not bucket or not key:
        app.logger.error(
            f"Missing results bucket/key for job_id={id}. "
        )
        abort(500)

    out_name = f'{job.get("input_file_name", "results")}.annotated.vcf'

    try:
        url = s3.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": bucket,
                "Key": key,
                "ResponseContentDisposition": f'attachment; filename="{out_name}"',
            },
            ExpiresIn=300,
        )
        return redirect(url)
    except (ClientError, BotoCoreError) as e:
        app.logger.error(f"Failed to presign results download for job_id={id}: {e}")
        abort(500)


@app.route("/annotations/<id>/log", methods=["GET"])
@authenticated
def annotation_log(id):
    user_id = current_user_id()
    job = get_job_or_404(id)
    ensure_job_owner(job, user_id)

    if job.get("job_status") != "COMPLETED":
        abort(403)

    input_file = job.get("input_file_name")
    if not input_file:
        abort(500)

    log_filename = f"{input_file}.count.log"
    log_key = f"{app.config['AWS_S3_KEY_PREFIX']}{user_id}/{id}/{log_filename}"

    try:
        obj = s3.get_object(
            Bucket=app.config["AWS_S3_RESULTS_BUCKET"],
            Key=log_key,
        )
        log_text = obj["Body"].read().decode("utf-8", errors="replace")

    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("NoSuchKey", "404"):
            log_text = "Log not available yet."
        else:
            app.logger.error(f"S3 get_object failed for log_key={log_key}: {e}")
            abort(500)
    except Exception as e:
        app.logger.error(f"Unexpected error retrieving log for job_id={id}: {e}")
        abort(500)

    return render_template("view_log.html", job=job, log=log_text)


import stripe
from auth import update_profile


@app.route("/subscribe", methods=["GET", "POST"])
def subscribe():
    app.logger.info(f"Stripe key prefix: {app.config.get('STRIPE_SECRET_KEY', '')[:7]}")
    if request.method == "GET":
        return render_template("subscribe.html")
    elif request.method == "POST":

        # Extracting stripe token
        stripe_token = request.form.get("stripe_token", "").strip()
        if not stripe_token:
            return render_template(
                "error.html",
                title="Subscription error",
                alert_level="danger",
                message="Missing Stripe token. Please try again.",
            ), 400

        # Getting the key and Ids of stripe
        stripe.api_key = app.config.get("STRIPE_SECRET_KEY")
        if not stripe.api_key:
            app.logger.error("Missing STRIPE_SECRET_KEY in config")
            abort(500)

        price_id = app.config.get("STRIPE_PRICE_ID")
        if not price_id:
            app.logger.error("Missing STRIPE_PRICE_ID in config")
            abort(500)

        # User identifiers from session
        user_id = session.get("primary_identity")
        user_email = session.get("email")
        user_name = session.get("name")
        app.logger.info(
            "Processing subscription for user_id=%s email=%s name=%s",
            user_id,
            user_email,
            user_name
        )

        # Creating Stripe Customer (card=stripe_token)
        try:
            customer = stripe.Customer.create(
                email=user_email,
                name=user_name,
                card=stripe_token,
                metadata={"user_id": user_id, "app": "GAS"},
            )
        except Exception as e:
            app.logger.error(f"Stripe Customer.create failed: {e}")
            return render_template(
                "error.html",
                title="Subscription error",
                alert_level="danger",
                message="Unable to create Stripe customer. Please try again.",
            ), 502
        app.logger.info("customer created=%s", customer)
        if not user_id or not user_email:
            abort(401)

        # Creating Subscription for the customer
        try:
            subscription = stripe.Subscription.create(
                customer=customer["id"],
                items=[{"price": price_id}],
                expand=["latest_invoice.payment_intent"],
            )
        except Exception as e:
            app.logger.error(f"Stripe Subscription.create failed: {e}")
            return render_template(
                "error.html",
                title="Subscription error",
                alert_level="danger",
                message="Unable to create Stripe subscription. Please try again.",
            ), 502

        # Updating user profile in accounts DB
        try:
            update_profile(identity_id=user_id, role="premium_user")
        except Exception as e:
            app.logger.error(f"update_profile failed for user_id={user_id}: {e}")
            abort(500)

        session["role"] = "premium_user"

        # Initiating thaw/restore of any archived results for the user
        try:
            started = initiate_glacier_restore(user_id)
            app.logger.info("Glacier restore initiated for user_id=%s count=%s", user_id, started)
        except Exception as e:
            app.logger.warning("Glacier restore initiation failed user_id=%s err=%s", user_id, str(e))

        thaw_queue = app.config.get("AWS_SQS_THAW_QUEUE_URL")

        try:
            scheduler = boto3.client("scheduler", region_name=app.config["AWS_REGION_NAME"])
            group = app.config.get("AWS_SCHEDULE_GROUP", "default")

            resp = jobs_table.query(
                IndexName="user_id_index",
                KeyConditionExpression=Key("user_id").eq(user_id),
            )
            jobs = resp.get("Items", [])

            for job in jobs:
                if job.get("job_status") != "COMPLETED":
                    continue
                if job.get("results_file_archive_id"):
                    continue

                try:
                    sqs.send_message(
                        QueueUrl=thaw_queue,
                        MessageBody=json.dumps({
                            "job_id": job["job_id"],
                            "user_id": user_id
                        })
                    )

                    app.logger.info(
                        "Sent thaw request job_id=%s user_id=%s",
                        job["job_id"],
                        user_id
                    )

                except Exception as e:
                    app.logger.error(
                        "Failed to enqueue thaw request job_id=%s error=%s",
                        job["job_id"],
                        str(e)
                    )

                ct = job.get("complete_time") or job.get("completed_time") or job.get("end_time")
                if ct is None:
                    continue

                if int(time.time()) - int(ct) > 15 * 60:
                    continue

                sched_name = f"a14-archive-{job['job_id']}"
                try:
                    scheduler.delete_schedule(Name=sched_name, GroupName=group)
                    app.logger.info(f"Deleted pending archive schedule {sched_name}")
                except scheduler.exceptions.ResourceNotFoundException:
                    pass
                except Exception as e:
                    app.logger.warning(f"Failed deleting schedule {sched_name}: {e}")

        except Exception as e:
            app.logger.warning(f"Cancel pending archival best-effort failed: {e}")

        return render_template(
            "subscribe_confirm.html",
            stripe_id=customer["id"]
        )

# FUnction for initiating restoring from glacier
def initiate_glacier_restore(user_id: str):
    glacier = boto3.client("glacier", region_name=app.config["AWS_REGION_NAME"])
    vault = app.config.get("AWS_GLACIER_VAULT", "ucmpcs")

    # Finding all jobs for the user
    resp = jobs_table.query(
        IndexName="user_id_index",
        KeyConditionExpression=Key("user_id").eq(user_id),
    )
    jobs = resp.get("Items", [])

    started = 0

    for job in jobs:
        job_id = job.get("job_id")
        archive_id = job.get("results_file_archive_id")

        if not job_id or not archive_id:
            continue

        if job.get("results_file_restore_job_id"):
            continue

        try:
            # Starting retrieval
            gj = glacier.initiate_job(
                vaultName=vault,
                jobParameters={
                    "Type": "archive-retrieval",
                    "ArchiveId": archive_id,
                    "Description": f"GAS restore results for job_id={job_id}",
                    "Tier": "Standard",
                    "SNSTopic": app.config["AWS_SNS_TOPIC_GLACIER"]
                },
            )
            restore_job_id = gj.get("jobId")
            if not restore_job_id:
                raise RuntimeError("initiate_job returned no jobId")

            # Persisting restore tracking info in DynamoDB
            jobs_table.update_item(
                Key={"job_id": job_id},
                UpdateExpression=(
                    "SET results_file_restore_job_id = :jid, "
                    "results_file_restore_status = :st, "
                    "results_file_restore_requested_time = :ts"
                ),
                ExpressionAttributeValues={
                    ":jid": restore_job_id,
                    ":st": "IN_PROGRESS",
                    ":ts": int(time.time()),
                },
            )

            started += 1
            app.logger.info(
                "Started Glacier restore user_id=%s job_id=%s archive_id=%s restore_job_id=%s",
                user_id, job_id, archive_id, restore_job_id
            )

        except Exception as e:
            app.logger.warning(
                "Failed to start Glacier restore user_id=%s job_id=%s archive_id=%s err=%s",
                user_id, job_id, archive_id, str(e)
            )

    return started

"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""


@app.route("/make-me-premium", methods=["GET"])
@authenticated
def make_me_premium():
    update_profile(identity_id=session["primary_identity"], role="premium_user")
    return redirect(url_for("profile"))


@app.route("/unsubscribe", methods=["GET"])
@authenticated
def unsubscribe():
    update_profile(identity_id=session["primary_identity"], role="free_user")
    return redirect(url_for("profile"))


@app.route("/", methods=["GET"])
def home():
    return render_template("home.html"), 200


@app.route("/login", methods=["GET"])
def login():
    app.logger.info(f"Login attempted from IP {request.remote_addr}")
    if request.args.get("next"):
        session["next"] = request.args.get("next")
    return redirect(url_for("authcallback"))


@app.errorhandler(404)
def page_not_found(e):
    return (
        render_template(
            "error.html",
            title="Page not found",
            alert_level="warning",
            message="The page you tried to reach does not exist.       Please check the URL and try again.",
        ),
        404,
    )


@app.errorhandler(403)
def forbidden(e):
    return (
        render_template(
            "error.html",
            title="Not authorized",
            alert_level="danger",
            message="You are not authorized to access this page.       If you think you deserve to be granted access, please contact the       supreme leader of the mutating genome revolutionary party.",
        ),
        403,
    )


@app.errorhandler(405)
def not_allowed(e):
    return (
        render_template(
            "error.html",
            title="Not allowed",
            alert_level="warning",
            message="You attempted an operation that's not allowed;       get your act together, hacker!",
        ),
        405,
    )


@app.errorhandler(500)
def internal_error(error):
    return (
        render_template(
            "error.html",
            title="Server error",
            alert_level="danger",
            message="The server encountered an error and could       not process your request.",
        ),
        500,
    )


from flask_wtf.csrf import CSRFError


@app.errorhandler(CSRFError)
def csrf_error(error):
    return (
        render_template(
            "error.html",
            title="CSRF error",
            alert_level="danger",
            message=f"Cross-Site Request Forgery error detected: {error.description}",
        ),
        400,
    )

### EOF