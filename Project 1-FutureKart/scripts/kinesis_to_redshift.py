# ''
# spark-submit \
#   --jars s3://awslabs-code-us-east-1/spark-sql-kinesis-connector/spark-streaming-sql-kinesis-connector_2.12-1.0.0.jar \
#   --packages \
# com.amazon.redshift:redshift-jdbc42:2.1.0.9,\
# io.github.spark-redshift-community:spark-redshift_2.12:6.4.3-spark_3.5,\
# org.apache.spark:spark-avro_2.12:3.5.1 \
#  kinesis_stream_to_redshift.py 
# ''


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, from_json
from pyspark.sql.types import StructType, StringType, IntegerType

# Create Spark session with Redshift driver and performance tuning
def create_spark_session():
    """Create optimized Spark session for streaming"""
    return SparkSession.builder \
        .appName("FutureKart-Worker-Safe-Pipeline") \
        .config("spark.jars.packages", "com.amazon.redshift:redshift-jdbc42:2.1.0.9") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

spark = create_spark_session()
spark.sparkContext.setLogLevel("ERROR")

# Define schema for case and survey data
case_schema = StructType() \
    .add("case_no", StringType()) \
    .add("create_timestamp", StringType()) \
    .add("last_modified_timestamp", StringType()) \
    .add("created_employee_key", StringType()) \
    .add("call_center_id", StringType()) \
    .add("status", StringType()) \
    .add("category", StringType()) \
    .add("sub_category", StringType()) \
    .add("communication_mode", StringType()) \
    .add("country_cd", StringType()) \
    .add("product_code", StringType())

survey_schema = StructType() \
    .add("survey_id", StringType()) \
    .add("case_no", StringType()) \
    .add("survey_timestamp", StringType()) \
    .add("q1", IntegerType()) \
    .add("q2", StringType()) \
    .add("q3", IntegerType()) \
    .add("q4", StringType()) \
    .add("q5", IntegerType())

# Read stream from Kinesis (no trailing space)
raw_stream = spark.readStream \
    .format("aws-kinesis") \
    .option("kinesis.region", "ap-south-1") \
    .option("kinesis.streamName", "project1_futurecart_case_survey_event_aahash") \
    .option("kinesis.consumerType", "GetRecords") \
    .option("kinesis.endpointUrl", "https://kinesis.ap-south-1.amazonaws.com") \
    .option("kinesis.startingposition", "LATEST") \
    .load()

# Convert JSON string to DataFrame
json_str_df = raw_stream.selectExpr("CAST(data AS STRING) as json_data")

# Separate survey records
survey_df = json_str_df \
    .filter(expr("json_data LIKE '%survey_id%'")) \
    .withColumn("parsed", from_json(col("json_data"), survey_schema)) \
    .select("parsed.*")

# Separate case records
case_df = json_str_df \
    .filter(expr("NOT json_data LIKE '%survey_id%'")) \
    .withColumn("parsed", from_json(col("json_data"), case_schema)) \
    .select("parsed.*")

# Redshift connection details (Aahash version)
redshift_jdbc_url = "jdbc:redshift://aahash-work.008673239246.ap-south-1.redshift-serverless.amazonaws.com:5439/dev"
redshift_user = "admin"
redshift_password = "Aahash123"
iam_role_arn = "arn:aws:iam::008673239246:role/Redshift-s3full-Aahash"  

# Function to write batch to Redshift
def write_to_redshift(df, batch_id, table_name, tempdir):
    if df.rdd.isEmpty():
        return
    print(f"Writing batch {batch_id} to table {table_name}, record count: {df.count()}")
    df.write \
        .format("io.github.spark_redshift_community.spark.redshift") \
        .option("url", f"{redshift_jdbc_url}?user={redshift_user}&password={redshift_password}") \
        .option("dbtable", table_name) \
        .option("tempdir", tempdir) \
        .option("aws_iam_role", iam_role_arn) \
        .option("driver", "com.amazon.redshift.jdbc.Driver") \
        .mode("append") \
        .save()

# Write case records to Redshift
case_query = case_df.writeStream \
    .foreachBatch(lambda df, id: write_to_redshift(df, id, "public.futurecart_case_details", "s3://aahash-project1/case/")) \
    .option("checkpointLocation", "s3://aahash-project1/checkpoints/case/") \
    .start()

# Write survey records to Redshift
survey_query = survey_df.writeStream \
    .foreachBatch(lambda df, id: write_to_redshift(df, id, "public.futurecart_survey_details", "s3://aahash-project1/survey/")) \
    .option("checkpointLocation", "s3://aahash-project1/checkpoints/survey/") \
    .start()

# Await termination
spark.streams.awaitAnyTermination()
