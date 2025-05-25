from pyspark.sql import SparkSession
from delta import *
from pyspark.sql.functions import current_timestamp, current_date, lit, col
import requests
import json
from datetime import datetime
import urllib.parse
import os
import uuid
from bs4 import BeautifulSoup

from metadata_manager import add_metadata_entry

BASE_PATH = 'file:///data/landing'
TMP_PATH = f'/data/tmp/counseling_reports'
os.makedirs(TMP_PATH, exist_ok=True)

def download_pdfs(base_url):
    response = requests.get(base_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    for link in soup.find_all('a'):
        if link.get('href') and not link.get('href').startswith('.'):
            pdf_url = base_url + link.get('href')
            filename = pdf_url.split("/")[-1]
            filepath = f'{TMP_PATH}/{filename}'
            response = requests.get(pdf_url)
            with open(filepath, "wb") as f:
                f.write(response.content)

def ingest_meeting_reports(api_url):
    source_name = 'meeting_reports'
    builder = SparkSession.builder \
        .appName("MeetingReportsIngestion") \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    batch_id = str(uuid.uuid4())
    ingestion_timestamp = datetime.now()
    ingestion_date = ingestion_timestamp.strftime("%Y-%m-%d")
    ingestion_time = ingestion_timestamp.strftime("%H-%M-%S")

    landing_path = f'{BASE_PATH}/{source_name}/date={ingestion_date}/batch={batch_id}'
    os.makedirs(landing_path, exist_ok=True)

    download_pdfs(api_url)

    df = spark.read \
        .format("binaryFile") \
        .option("pathGlobFilter", "*.pdf") \
        .load(TMP_PATH)
    
    df = df.withColumn("_source_name", lit(source_name)) \
           .withColumn("_batch_id", lit(batch_id)) \
           .withColumn("_ingestion_timestamp", lit(ingestion_timestamp.isoformat())) \
           .withColumn("_ingestion_date", lit(ingestion_date))
    
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(landing_path)
    print(f'Successfully ingested data to {landing_path}')

    record_count = df.count()
    metadata = {
        "source_name": source_name,
        "batch_id": batch_id,
        "api_url": api_url,
        "ingestion_timestamp": ingestion_timestamp.isoformat(),
        "ingestion_date": ingestion_date,
        "record_count": record_count,
        "landing_path": landing_path
    }
    add_metadata_entry(metadata)

    tmp_files = os.listdir(TMP_PATH)
    for tmp_file in tmp_files:
        tmp_filepath = os.path.join(TMP_PATH, tmp_file)
        if os.path.isfile(tmp_filepath):
            os.remove(tmp_filepath)
        
    spark.sparkContext.stop()

    return metadata

if __name__ == '__main__':
    API_URL = 'http://counseling-reports:8000/'
    metadata = ingest_meeting_reports(API_URL)
    print(f'Ingestion completed with batch ID: {metadata["batch_id"]}')