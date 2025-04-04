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

def ingest_meeting_requests():
    source_name = 'meeting_requests'
    builder = SparkSession.builder \
        .appName("MeetingRequestsIngestion") \
        .config("spark.driver.host", "delta-lake-airflow-worker-1") \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    ingestion_timestamp = datetime.now()
    ingestion_date = ingestion_timestamp.strftime("%Y-%m-%d")

    temporal_path = f'{TEMPORAL_ZONE}/{source_name}'

    schema = StructType() \
        .add("student_id", StringType()) \
        .add("purpose", StringType()) \
        .add("description", StringType()) \
        .add("requested_at", TimestampType())
    
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "counselor-requests") \
        .option("startingOffsets", "latest") \
        .load()
    
    df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
        .withColumn("json_data", from_json(col("json_str"), schema)) \
        .select("json_data.*") \
        .withColumn("date", to_date("requested_at"))
    print('Stream read successfully')
    
    query = df_parsed.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", CHECKPOINTS) \
        .partitionBy("date") \
        .start(temporal_path)
    print(f'Stream written successfully to {temporal_path}')
    
    query.awaitTermination()
    metadata = {
        "source_name": source_name,
        "ingestion_timestamp": ingestion_timestamp.isoformat(),
        "ingestion_date": ingestion_date,
        "temporal_path": temporal_path,
        "process_status": "INGESTED_TO_TEMPORAL"
    }
    add_metadata_entry(metadata)

    spark.sparkContext.stop()

    return metadata

if __name__ == '__main__':
    metadata = ingest_meeting_requests()
    print('Ingestion completed')