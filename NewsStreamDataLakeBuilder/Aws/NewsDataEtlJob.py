import sys
from awsglue.transforms import ApplyMapping
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
spark_context = SparkContext()
glue_context = GlueContext(spark_context)
spark_session = glue_context.spark_session
etl_job = Job(glue_context)
etl_job.init(args["JOB_NAME"], args)

# Extract data from AWS Glue Data Catalog
news_data_catalog = glue_context.create_dynamic_frame.from_catalog(
    database="news-db",
    table_name="news",
    transformation_ctx="news_data_catalog",
)

# Transform schema of extracted data
transformed_news_data = ApplyMapping.apply(
    frame=news_data_catalog,
    mappings=[
        ("source.Id", "string", "source_id", "string"),
        ("source.Name", "string", "source_name", "string"),
        ("author", "string", "author", "string"),
        ("title", "string", "title", "string"),
    ],
    transformation_ctx="transformed_news_data",
)

# Convert the DynamicFrame to a Spark DataFrame for loading into RDS
news_dataframe = transformed_news_data.toDF()

db_connection_options = glue_context.extract_jdbc_conf("news-db-connection")

# Write DataFrame to RDS
news_dataframe.write \
  .format("jdbc") \
  .option("url", db_connection_options["fullUrl"]) \
  .option("dbtable", "news") \
  .option("user", db_connection_options["user"]) \
  .option("password", db_connection_options["password"]) \
  .save()

etl_job.commit()