import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import current_timestamp

# --- Initialize Glue Job ---
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --- Read from Aurora MySQL ---
MySQL_node = glueContext.create_dynamic_frame.from_options(
    connection_type="mysql",
    connection_options={
        "useConnectionProperties": "true",
        "dbtable": "sb_node_ott.meta_information",
        "connectionName": "cms sandbox connection",
    },
    transformation_ctx="MySQL_node"
)

# --- Convert to DataFrame for transformation ---
df = MySQL_node.toDF()

# ✅ Add modified_at column with current timestamp
df_with_time = df.withColumn("modified_at", current_timestamp())

# --- Convert back to DynamicFrame ---
final_dyf = DynamicFrame.fromDF(df_with_time, glueContext, "final_dyf")

# --- Write to S3 in Parquet format ---
s3_sink = glueContext.getSink(
    path="s3://pb-etl-athena/analytics_etl/Pb_meta_information/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="s3_sink"
)
s3_sink.setCatalogInfo(
    catalogDatabase="analyticsdatabase",
    catalogTableName="Pb_meta_information"
)
s3_sink.setFormat("glueparquet", compression="snappy")
s3_sink.writeFrame(final_dyf)

# --- Final Commit ---
job.commit()
