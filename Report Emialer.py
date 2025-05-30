import sys
import boto3
import pandas as pd
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
from datetime import datetime, timedelta

# Current date
today = datetime.today().strftime('%d %b %Y')

# Yesterday's date
yesterday = (datetime.today() - timedelta(days=1)).strftime('%d %b %Y')

# --- Step 1: Parse job arguments ---
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'aws_ses_host', 'aws_ses_username', 'aws_ses_password'])

# --- Step 2: Initialize Spark & Glue ---
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
logger = glueContext.get_logger()

# --- Step 3: Email config ---
my_host = args['aws_ses_host']
my_port = 587
my_access_key = args['aws_ses_username']
my_secret = args['aws_ses_password']

# --- Step 4: Athena + S3 Constants ---
ATHENA_DATABASE = "analyticsdatabase"
S3_BUCKET = "pb-ott-athena"
REGION_NAME = "ap-south-1"
current_date_str = datetime.today().strftime("%d %b %Y")

# --- Step 5: Query Template Paths ---
QUERY_TEMPLATE_02 = "query_templates/platform_wise_users_v2.sql"
QUERY_TEMPLATE_03 = "query_templates/BBNL & BSNL Users_v2.sql"
QUERY_TEMPLATE_04 = "query_templates/platform_wise_downloads_athena.sql"

# --- Step 6: Load SQL from S3 ---
s3 = boto3.client('s3')

def get_query_template(bucket, queryTemplatePath):
    response = s3.get_object(Bucket=bucket, Key=queryTemplatePath)
    return response["Body"].read().decode("utf-8").strip()

query02 = get_query_template(S3_BUCKET, QUERY_TEMPLATE_02)
query03 = get_query_template(S3_BUCKET, QUERY_TEMPLATE_03)
query04 = get_query_template(S3_BUCKET, QUERY_TEMPLATE_04)

logger.info("Loaded Queries")

# --- Step 7: Define Table Headers ---
headers_users = ["Platform", f"{yesterday}", f"{today}", "Growth", "Growth %"]
headers_providers = ["Provider",f"{yesterday}", f"{today}", "Growth", "Growth %"]
headers_athena_downloads = ["Platform", f"{yesterday}",f"{today}", "Growth", "Growth %"]

# --- Step 8: Execute Queries ---
def run_mysql_query(query):
    dynamic_df = glueContext.create_dynamic_frame.from_options(
        connection_type="mysql",
        connection_options={
            "useConnectionProperties": "true",
            "dbtable": "pb_node_ott.users",
            "connectionName": "Aurora connection",
            "sampleQuery": query
        },
        transformation_ctx="MySQL_dynamic_frame"
    )
    return dynamic_df.toDF().toPandas()

def run_athena_query(query):
    athena = boto3.client('athena', region_name=REGION_NAME)
    output_location = f"s3://{S3_BUCKET}/athena_results/"
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        ResultConfiguration={"OutputLocation": output_location}
    )
    query_execution_id = response['QueryExecutionId']

    # Wait for query to complete
    while True:
        status = athena.get_query_execution(QueryExecutionId=query_execution_id)['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break

    if status != 'SUCCEEDED':
        raise Exception("Athena query failed")

    result = athena.get_query_results(QueryExecutionId=query_execution_id)
    headers = [col['VarCharValue'] for col in result['ResultSet']['Rows'][0]['Data']]
    rows = [[col.get('VarCharValue', '') for col in row['Data']] for row in result['ResultSet']['Rows'][1:]]
    return pd.DataFrame(rows, columns=headers)

# --- Step 9: Convert pandas to HTML ---
def pandas_df_to_html(pandas_df, headers):
    html = "<table border='1' style='border-collapse: collapse;'>"
    html += "<tr>" + "".join([f"<th>{col}</th>" for col in headers]) + "</tr>"
    for _, row in pandas_df.iterrows():
        html += "<tr>" + "".join([f"<td>{val}</td>" for val in row]) + "</tr>"
    html += "</table><br>"
    return html

# --- Step 10: Generate HTML Blocks ---
html2 = pandas_df_to_html(run_mysql_query(query02), headers_users)
html3 = pandas_df_to_html(run_mysql_query(query03), headers_providers)
html4 = pandas_df_to_html(run_athena_query(query04), headers_athena_downloads)

final_html = f"<h2>Registered Users - {current_date_str}</h2>" + html2
final_html += f"<h2>BBNL & BSNL Users - {current_date_str}</h2>" + html3
final_html += f"<h2>Platform-wise Downloads  - {current_date_str}</h2>" + html4

logger.info("Final HTML Generated")

# --- Step 11: Send Email ---
sender_email = "WavesPB Admin <no-reply@wavespb.com>"
receiver_email = "amiteshawasthi7@gmail.com"
subject = f"Daily Report - Platform Wise Stats | {current_date_str}"

msg = MIMEMultipart("alternative")
msg['Subject'] = subject
msg['From'] = sender_email
msg['To'] = receiver_email
msg.attach(MIMEText(final_html, "html"))

try:
    with smtplib.SMTP(my_host, my_port) as server:
        server.starttls()
        server.login(my_access_key, my_secret)
        server.sendmail(sender_email, receiver_email, msg.as_string())
        logger.info("Mail sent successfully to " + receiver_email)
except Exception as e:
    logger.error("Mail send failed: " + str(e))

# --- Step 12: Finish Job ---
job.commit()
