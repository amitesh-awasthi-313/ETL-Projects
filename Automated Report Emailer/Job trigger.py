import boto3
import time
from botocore.exceptions import ClientError

# --- Job Names ---
job1 = "pb_meta_info"
job2 = "PB_REGISTERED_USERS_DOWNLOADS_MAILER"

# --- STEP 1: IAM Credentials and Config ---
aws_access_key_id = "Your_ARn_Accesskey"
aws_secret_access_key = "Your_secret_key"
region_name = "ap-south-1"
assume_role_arn = "arn:aws:iam::905418019108:role/Role_STS"
session_name = "AssumeRoleManualSession"

# --- STEP 2: Assume Role using STS ---
sts_client = boto3.client(
    "sts",
    region_name=region_name,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

try:
    response = sts_client.assume_role(
        RoleArn=assume_role_arn,
        RoleSessionName=session_name
    )
    temp_credentials = response["Credentials"]
    print(" Temporary credentials fetched successfully.")
except ClientError as e:
    print(f" Failed to assume role: {e.response['Error']['Message']}")
    raise

# --- STEP 3: Initialize Glue Client with Temp Credentials ---
glue_client = boto3.client(
    "glue",
    region_name=region_name,
    aws_access_key_id=temp_credentials["AccessKeyId"],
    aws_secret_access_key=temp_credentials["SecretAccessKey"],
    aws_session_token=temp_credentials["SessionToken"]
)

# --- STEP 4: Function to Run a Glue Job and Wait Until Completion ---
def run_glue_job(job_name):
    try:
        start_response = glue_client.start_job_run(JobName=job_name)
        run_id = start_response["JobRunId"]
        print(f" Started job '{job_name}' with Run ID: {run_id}")
    except ClientError as e:
        print(f" Failed to start Glue job {job_name}: {e.response['Error']['Message']}")
        raise

    # Poll for job status
    while True:
        job_status = glue_client.get_job_run(JobName=job_name, RunId=run_id)
        state = job_status["JobRun"]["JobRunState"]
        print(f"⏳ Job '{job_name}' status: {state}")

        if state in ["SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT", "ERROR", "EXPIRED"]:
            break
        time.sleep(5)

    if state != "SUCCEEDED":
        raise Exception(f" Glue job '{job_name}' failed with status: {state}")
    print(f"✅ Glue job '{job_name}' completed successfully.")

# --- STEP 5: Run Two Jobs Sequentially ---
run_glue_job(job1)
run_glue_job(job2)
