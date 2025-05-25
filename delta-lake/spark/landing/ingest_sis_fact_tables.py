from pyspark.sql import SparkSession
from delta import *
from pyspark.sql.functions import current_timestamp, current_date, lit, col
from datetime import datetime
import uuid
import os
import json

from metadata_manager import add_metadata_entry

BASE_PATH = 'file:///data/landing'
FACT_TABLES = ['dropouts', 'enrollment', 'evaluations', 'graduations', 'leave_of_absence', 'student_enrollment']

def init_spark():
    builder = SparkSession.builder \
        .appName("SISDimensionTablesIngestion") \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark

def ingest_data(spark, source_name, table_name):
    print(f'[INGESTION TASK] Ingesting data for source: {source_name}')
    batch_id = str(uuid.uuid4())
    ingestion_timestamp = datetime.now()
    ingestion_date = ingestion_timestamp.strftime("%Y-%m-%d")

    landing_path = f'{BASE_PATH}/{source_name}/date={ingestion_date}/batch={batch_id}'
    os.makedirs(landing_path, exist_ok=True)

    df = spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_sis:5432/university")\
        .option("dbtable", table_name)\
        .option("user", "admin") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .load()
    
    df = df.withColumn("_source_name", lit(source_name)) \
           .withColumn("_batch_id", lit(batch_id)) \
           .withColumn("_ingestion_timestamp", lit(ingestion_timestamp.isoformat())) \
           .withColumn("_ingestion_date", lit(ingestion_date))
    
    df.write \
        .format('delta') \
        .mode('overwrite') \
        .save(landing_path)
    print(f'[INGESTION TASK] Successfully ingested data to {landing_path}')

    record_count = df.count()
    metadata = {
        "source_name": source_name,
        "batch_id": batch_id,
        "ingestion_timestamp": ingestion_timestamp.isoformat(),
        "ingestion_date": ingestion_date,
        "record_count": record_count,
        "landing_path": landing_path,
    }
    add_metadata_entry(metadata)

    print(f'[INGESTION TASK] Ingestion completed for source {source_name} with batch ID: {metadata["batch_id"]}')

if __name__ == '__main__':
    spark = init_spark()
    for table in FACT_TABLES:
        ingest_data(spark, f'sis_{table}', table)
    
    spark.sparkContext.stop()