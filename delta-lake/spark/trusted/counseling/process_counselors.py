from pyspark.sql import SparkSession
from delta import *
from pyspark.sql.functions import current_timestamp, current_date, lit, col
import json
from datetime import datetime
import os
import uuid
import glob
from pymongo import MongoClient

from preprocess.preprocess_counselors import preprocessing_pipeline

MONGO_URI = 'mongodb://root:root@counseling-db:27017/counseling?authSource=admin'
mongo_client = MongoClient(MONGO_URI)
db = mongo_client['counseling']
counselors_collection = db['counselors']

BASE_LANDING_PATH = '/data/landing'

def init_spark():
    builder = SparkSession.builder \
        .appName("CounselorsProcessing") \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark

def read_data(spark, source_name):
    landing_dir = f'{BASE_LANDING_PATH}/{source_name}'
    all_batches = glob.glob(os.path.join(landing_dir, 'date=*', 'batch=*'))
    latest_batch_path = max(all_batches, key=os.path.getmtime)
    abs_path = os.path.abspath(latest_batch_path)
    batch_id = os.path.basename(abs_path).split('=')[1]

    df = spark.read \
        .format('delta') \
        .load(abs_path)

    return df, batch_id

def process_data(df):
    print(f'[PROCESSING TASK] Preprocessing began')
    df = preprocessing_pipeline(df)
    print(f'[PROCESSING TASK] Preprocessing completed')

    process_timestamp = datetime.now()
    process_date = process_timestamp.strftime("%Y-%m-%d")
    process_time = process_timestamp.strftime("%H-%M-%S")
    df = df.withColumn("_process_timestamp", lit(process_timestamp.isoformat())) \
           .withColumn("_process_date", lit(process_date))
    
    clean_counselors = [json.loads(row) for row in df.toJSON().collect()]
    counselors_collection.insert_many(clean_counselors)
    print(f'[PROCESSING TASK] Successfully inserted {df.count()} documents of {source_name}')

if __name__ == '__main__':
    spark = init_spark()
    source_name = 'counselor_api'
    df, batch_id = read_data(spark, source_name)
    process_data(df)
    spark.sparkContext.stop()
    print(f'[PROCESSING TASK] Processing completed for {source_name} batch ID {batch_id}')