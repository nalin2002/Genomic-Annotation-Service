# GAS Framework
An enhanced web framework (based on [Flask](https://flask.palletsprojects.com/)) for use in the capstone project. Adds robust user authentication (via [Globus Auth](https://docs.globus.org/api/auth)), modular templates, and some simple styling based on [Bootstrap](https://getbootstrap.com/docs/3.3/).

Directory contents are as follows:
* `/web` - The GAS web app files
* `/ann` - Annotator files
* `/util` - Utility scripts/apps for notifications, archival, and restoration
* `/aws` - AWS user data files


Project Implemented:

When a user upgrades from a Free account to a Premium account, any of their previously archived result 
files must be restored from Glacier so that they can again download their data. The thawing and 
restoration pipeline was designed as an asynchronous, event-driven workflow so that the web server does 
not block while waiting for long-running Glacier retrieval jobs.

### Sequence of Events:

1.	User Upgrade (Web Instance):

When a user subscribes and their role changes to premium_user, the web server checks the user’s 
completed jobs in DynamoDB. For any job that has an archived result file (results_file_archive_id present), 
a message containing the job ID and user ID is pushed to the thaw request SQS queue. 
This decouples the web server from the restoration process and allows it to respond 
immediately to the user without waiting for Glacier retrieval.

2. Thaw Request Processing (Utility Instance):

A background utility (thaw_script.py) running on the utility EC2 instance continuously polls the 
thaw queue using long polling. When a thaw request is received, the script initiates a Glacier archive
retrieval job using the stored archive_id. The code first attempts an Expedited retrieval and if unsuccessful, the request is retried using a Standard retrieval.

3. Tracking Retrieval Status:

When the retrieval job is initiated, the Glacier job_id is stored in DynamoDB under 
results_file_restore_job_id. This allows the system to track the restoration state. 
While the retrieval job is in progress, the web interface displays the message “File is being restored; please check back later” 
when the user views the job details page.

4.	Completion Notification and Restoration (Lambda):

Once the Glacier retrieval job completes, a notification is published to an SNS topic configured in the 
retrieval request. This SNS event triggers the AWS Lambda restoration function (nalinprabhath-a16-restore).
The Lambda function retrieves the thawed archive using the Glacier API, uploads the restored results 
file back into the gas-results S3 bucket under the user’s directory, deletes the archive from 
Glacier, and updates the corresponding DynamoDB record to reflect that the file is now restored.
   
5.	User Access Restored:

After DynamoDB is updated and the file is copied back to S3, the job details page 
will again display the download link, allowing the Premium user to access their results file normally.

### Design Rationale:

-	Scalability:
Long-running Glacier operations are handled asynchronously using SQS queues and background 
utilities. This prevents the web server from blocking and allows the system to handle multiple 
restoration requests concurrently.
- Loose Coupling:
Each component in the workflow (web server, thaw utility, Glacier retrieval, Lambda restoration) 
operates independently and communicates through messaging services such as SQS and SNS. 
This reduces direct dependencies and allows components to be modified or scaled independently.
-	Reliability and Graceful Degradation:
The system first attempts an Expedited Glacier retrieval for faster access but 
automatically falls back to a Standard retrieval if expedited capacity is unavailable. 
This ensures the restore operation still succeeds even when optimal resources are not available.
-	Consistency of State:
DynamoDB serves as the source of truth for job status and restoration metadata. 
Each stage of the pipeline updates the job record, ensuring the UI accurately reflects 
the current state of the data (archived, restoring, or available).
-	User Experience:
Instead of leaving users unaware of long restoration times, the UI explicitly indicates 
when a file is being restored. This improves transparency and avoids confusion during 
Glacier’s multi-hour retrieval window.

Overall, the use of asynchronous messaging, event-driven triggers, and serverless restoration 
provides a scalable and reliable approach for managing archived data restoration 
while maintaining a responsive user interface.



