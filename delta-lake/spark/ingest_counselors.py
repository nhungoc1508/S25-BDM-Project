from pyspark.sql import SparkSession
from delta import *
from pyspark.sql.functions import current_timestamp, current_date, lit, col
import requests
import json
from datetime import datetime
import os
import uuid

from metadata_manager import add_metadata_entry

BASE_PATH = 'file:///data/landing'
TEMPORAL_ZONE = f'{BASE_PATH}/temporal'
PERSISTENT_ZONE = f'{BASE_PATH}/persistent'
TMP_PATH = f'/data/tmp'
os.makedirs(TMP_PATH, exist_ok=True)

def ingest_json_to_temporal(api_url):
    source_name = "counselor_api"
    builder = SparkSession.builder \
        .appName("CounselorsIngestion") \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    batch_id = str(uuid.uuid4())
    ingestion_timestamp = datetime.now()
    ingestion_date = ingestion_timestamp.strftime("%Y-%m-%d")
    ingestion_time = ingestion_timestamp.strftime("%H-%M-%S")

    temporal_path = f'{TEMPORAL_ZONE}/{source_name}/date={ingestion_date}/batch={batch_id}'
    os.makedirs(temporal_path, exist_ok=True)

    response = requests.get(api_url)
    if response.status_code != 200:
        raise Exception(f"Counselor API request failed with status code {response.status_code}")
    json_data = response.json()

    temp_file_path = f'{TMP_PATH}/{source_name}_{ingestion_date}_{ingestion_time}.json'
    with open(temp_file_path, "w") as f:
        if isinstance(json_data, list):
            for item in json_data:
                f.write(json.dumps(item) + "\n")
        else:
            f.write(json.dumps(json_data))

    df = spark.read \
        .option("multiLine", "false") \
        .option("inferSchema", "true") \
        .json(temp_file_path)
    
    df = df.withColumn("_source_name", lit(source_name)) \
           .withColumn("_batch_id", lit(batch_id)) \
           .withColumn("_ingestion_timestamp", lit(ingestion_timestamp.isoformat())) \
           .withColumn("_ingestion_date", lit(ingestion_date))
    
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(temporal_path)
    print(f'Successfully ingested data to {temporal_path}')
    
    record_count = df.count()
    metadata = {
        "source_name": source_name,
        "batch_id": batch_id,
        "api_url": api_url,
        "ingestion_timestamp": ingestion_timestamp.isoformat(),
        "ingestion_date": ingestion_date,
        "record_count": record_count,
        "temporal_path": temporal_path,
        "process_status": "INGESTED_TO_TEMPORAL"
    }
    add_metadata_entry(metadata)
    
    os.remove(temp_file_path)
    spark.sparkContext.stop()

    return metadata

if __name__ == '__main__':
    API_URL = 'http://fastapi_counselor_data:8000/counselors'
    metadata = ingest_json_to_temporal(API_URL)
    print(f'Ingestion completed with batch ID: {metadata["batch_id"]}')