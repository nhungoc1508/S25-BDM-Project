from pyspark.sql import SparkSession
from delta import *
from pyspark.sql.functions import col, lit
from datetime import datetime, timedelta
from metadata_manager import get_batches_by_status, update_metadata_for_promotion, update_metadata_for_failed_promotion

BASE_PATH = 'file:///data/landing'
TEMPORAL_ZONE = f'{BASE_PATH}/temporal'
PERSISTENT_ZONE = f'{BASE_PATH}/persistent'

def promote_data_to_persistent():
    builder = SparkSession.builder \
        .appName("PersistentPromotion") \
        .config("spark.driver.host", "delta-lake-airflow-worker-1") \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    batches_to_promote = get_batches_by_status("INGESTED_TO_TEMPORAL")
    print(f'Found {len(batches_to_promote)} batches to promote')
    
    for batch in batches_to_promote:
        source_name = batch["source_name"]
        batch_id = batch["batch_id"]
        ingestion_date = batch["ingestion_date"]
        temporal_path = batch["temporal_path"]
        print(f'Processing source {source_name}, batch {batch_id}')

        persistent_path = f"{PERSISTENT_ZONE}/{source_name}/ingested={ingestion_date}/batch={batch_id}"

        try:
            df = spark.read.format("delta").load(temporal_path)
            promotion_timestamp = datetime.now()
            df = df.withColumn("_promotion_timestamp", lit(promotion_timestamp.isoformat()))
            df.write \
              .format("delta") \
              .mode("overwrite") \
              .save(persistent_path)
            print(f'Successfully promoted data to {persistent_path}')
            update_metadata_for_promotion(batch_id, persistent_path)
        except Exception as e:
            error_msg = str(e)
            print(f'Error processing batch {batch_id}: {error_msg}')
            update_metadata_for_failed_promotion(batch_id, error_msg)

    spark.sparkContext.stop()

if __name__ == '__main__':
    promote_data_to_persistent()