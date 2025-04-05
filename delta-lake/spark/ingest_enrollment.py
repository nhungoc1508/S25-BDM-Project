from pyspark.sql import SparkSession
from delta import *
from pyspark.sql.functions import current_timestamp, current_date, lit, col
from datetime import datetime
import uuid

from metadata_manager import add_metadata_entry

BASE_PATH = 'file:///data/landing'
TEMPORAL_ZONE = f'{BASE_PATH}/temporal'
PERSISTENT_ZONE = f'{BASE_PATH}/persistent'
TMP_PATH = f'/data/tmp'

def ingest_enrollment_data():
    source_name = 'sis_enrollment'
    builder = SparkSession.builder \
        .appName("EnrollmentIngestion") \
        .config("spark.driver.host", "delta-lake-airflow-worker-1") \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    batch_id = str(uuid.uuid4())
    ingestion_timestamp = datetime.now()
    ingestion_date = ingestion_timestamp.strftime("%Y-%m-%d")
    ingestion_time = ingestion_timestamp.strftime("%H-%M-%S")

    temporal_path = f'{TEMPORAL_ZONE}/{source_name}/date={ingestion_date}/batch={batch_id}'

    df = spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_sis:5432/university")\
        .option("dbtable", "enrollment")\
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
        .save(temporal_path)
    print(f'Successfully ingested data to {temporal_path}')

    record_count = df.count()
    metadata = {
        "source_name": source_name,
        "batch_id": batch_id,
        "ingestion_timestamp": ingestion_timestamp.isoformat(),
        "ingestion_date": ingestion_date,
        "record_count": record_count,
        "temporal_path": temporal_path,
        "process_status": "INGESTED_TO_TEMPORAL"
    }
    add_metadata_entry(metadata)

    spark.sparkContext.stop()

    return metadata

if __name__ == '__main__':
    metadata = ingest_enrollment_data()
    print(f'Ingestion completed with batch ID: {metadata["batch_id"]}')