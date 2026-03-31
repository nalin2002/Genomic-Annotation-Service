## GAS Web Server
This directory contains the Flask-based web app for the GAS.

Add code to `views.py` and add/update Jinja2 templates in `/templates`. Your constants (e.g., queue names) must be declared in `config.py` and accessed via the `app.config` object.

Your web server must listen for requests on port 4433, as defined in `run_gas.sh`.

## Sequence of events:
1. Job Submission
	-	A user submits an annotation job.
	-	The job metadata is stored in DynamoDB with status PENDING.
2. Job Completion (Annotator Instance)
	-	After processing, the annotator upoads the results file and log file to S3 results bucket.
	-	Updates the DynamoDB job item status to COMPLETED, complete_time ,result keys.
	-	Publishes a job completion notification to the nalinprabhath_a14_job_results SNS topic.
3. Notification Utility (util instance – notify.py)
	-	The notification utility polls the results SQS queue using long polling.
	-	Upon receiving a completion message, it creates a one-time EventBridge Scheduler job that will trigger exactly 3 minutes after complete_time.
	-	The scheduler is configured to send a message to the archive SQS queue (nalinprabhath_a14_archive_requests).
	-	The schedule auto-deletes itself after execution.
4. Archive Utility (archive_script.py)
	-	The archive utility continuously long-polls the archive queue.
	-	When a scheduled message arrives, it retrieves the job from DynamoDB and checks the user’s role via the accounts database.
	-	If the user is still free_user, it allows downloads of results file from S3 for 3min and after 3 min, the file is archived in glacier vault umpcs. The archiveID is also stored in DynamoDB.
	-	If the user has upgraded to Premium before execution,  archival doesn't occurs.
	-	The SQS message is deleted only after successful processing.
5. Web Server Behavior
	-	On the job details page, If results_file_archive_id exists and user role is free_user, the “download” link is replaced with "upgrade to Premium for download" which points to /make-me-premium.
	-	Premium users always retain download access.

## Design Rationale (Why):
1. Scalability: 
I avoided sleep() delays , continuous database polling , delayed SQS delivery , visibility timeouts as these approaches do not scale because they block compute resources or create unnecessary load.
. Instead, I used EventBridge Scheduler to create one-time delayed executions. This approach scales because it offloads timing responsibility to AWS infrastructure and allows the system to scale horizontally without increasing CPU or memory usage on application instances.
This ensures the system can handle a large number of concurrent jobs without increasing CPU utilization.


2. Consistency and Correctness:
The archival decision is made at execution time, not at job completion time. The archive utility retrieves the current user role from the accounts database before archiving.
This guarantees that:
	-	If a user upgrades within the 3-minute window, archival does not occur.
	-	No stale role data is used.
	-	The accounts database remains the single source of truth.

I did not replicate user role information in DynamoDB to avoid data duplication and inconsistency.

3. Reliability:
	SQS messages are deleted only after successful archival. If any AWS operation fails (S3, Glacier, DynamoDB), the message remains in the queue and can be retried. This provides at-least-once delivery semantics and prevents silent data loss.
Each scheduler job is uniquely named per job ID to prevent collisions and ensure safe retries

4. Cost Efficiency:
As provided in the exercise, Glacier is right for our archival rather than S3 because of its low cost archival storage.

5. User Experience:
From the user’s perspective:
	-	Free users have access for 3 minutes.
	-	After archival, the UI clearly indicates that upgrading restores access.
	-	Premium users are never interrupted.
	-	The system behaves deterministically and transparently.


- Corner Cases Handled
	-	User upgrades before scheduler executes → no archival.
	-	Missing S3 object → archival skipped safely.
	-	AWS errors → message remains in queue.
	-	Duplicate schedule prevention via unique schedule names per job.


