from pyspark.sql import SparkSession
from delta import *
from pyspark.sql.functions import current_timestamp, current_date, lit, col, from_json, col, to_date
from pyspark.sql.types import StructType, StringType, TimestampType
from datetime import datetime

from metadata_manager import add_metadata_entry

BASE_PATH = 'file:///data/landing'
TEMPORAL_ZONE = f'{BASE_PATH}/temporal'
PERSISTENT_ZONE = f'{BASE_PATH}/persistent'
TMP_PATH = f'/data/tmp'
CHECKPOINTS = f'{BASE_PATH}/checkpoints/meeting_requests'

KAFKA_TOPIC_REQUESTS = 'meeting-requests'
KAFKA_TOPIC_REPORTS = 'meeting-reports'

def ingest_meeting_requests_reports():
    source_name_requests = 'meeting_requests'
    source_name_reports = 'meeting_reports'
    builder = SparkSession.builder \
        .appName("MeetingRequestsIngestion") \
        .config("spark.driver.host", "delta-lake-airflow-worker-1") \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    ingestion_timestamp = datetime.now()
    ingestion_date = ingestion_timestamp.strftime("%Y-%m-%d")

    temporal_path_requests = f'{TEMPORAL_ZONE}/{source_name_requests}'
    temporal_path_reports = f'{TEMPORAL_ZONE}/{source_name_reports}'

    schema_request = StructType() \
        .add("request_id", StringType()) \
        .add("student_id", StringType()) \
        .add("purpose", StringType()) \
        .add("description", StringType()) \
        .add("request_timestamp", TimestampType()) \
        .add("requestor_type", StringType())
    
    schema_report = StructType() \
        .add("meeting_id", StringType()) \
        .add("student_id", StringType()) \
        .add("counselor_id", StringType()) \
        .add("meeting_date", TimestampType()) \
        .add("report_content", StringType())
    
    request_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", KAFKA_TOPIC_REQUESTS) \
        .option("startingOffsets", "latest") \
        .load()
    
    report_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", KAFKA_TOPIC_REPORTS) \
        .option("startingOffsets", "latest") \
        .load()
    
    request_parsed = request_stream.selectExpr("CAST(value AS STRING) as json_str") \
        .withColumn("json_data", from_json(col("json_str"), schema_request)) \
        .select("json_data.*") \
        .withColumn("date", to_date("request_timestamp"))
    print('Request stream read successfully')

    report_parsed = report_stream.selectExpr("CAST(value AS STRING) as json_str") \
        .withColumn("json_data", from_json(col("json_str"), schema_report)) \
        .select("json_data.*") \
        .withColumn("date", to_date("meeting_date"))
    print('Report stream read successfully')
    
    request_query = request_parsed.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", CHECKPOINTS) \
        .partitionBy("date") \
        .start(temporal_path_requests)
    print(f'Stream written successfully to {temporal_path_requests}')

    report_query = report_parsed.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", CHECKPOINTS) \
        .partitionBy("date") \
        .start(temporal_path_reports)
    print(f'Stream written successfully to {temporal_path_reports}')
    
    request_query.awaitTermination()
    report_query.awaitTermination()

    spark.sparkContext.stop()

    return metadata

if __name__ == '__main__':
    metadata = ingest_meeting_requests_reports()
    print('Ingestion completed')