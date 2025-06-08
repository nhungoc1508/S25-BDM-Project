import os
import glob
import duckdb
import pandas as pd
from pyspark.sql import SparkSession
from preprocess.preprocess_evaluation import preprocessing_pipeline
from metadata_manager import add_metadata_entry  # Assuming same metadata system is used
from datetime import datetime
import logging

log = logging.getLogger(__name__)


# Spark Session
spark = SparkSession.builder.appName("EvaluationsProcessing").getOrCreate()

# Path config
landing_path = '/data/landing/sis_evaluations/'

# Find the latest batch folder
all_batches = glob.glob(os.path.join(landing_path, 'date=*', 'batch=*'))
latest_batch_path = max(all_batches, key=os.path.getmtime)

# # Read the data
abs_path = os.path.abspath(latest_batch_path)
df_eval = spark.read.format("parquet").load(f'file://{abs_path}')

# Convert to Pandas and clean
pdf_eval = df_eval.toPandas()
pdf_eval = preprocessing_pipeline(pdf_eval)

log.info(f"Length of pdf_eval: {len(pdf_eval)}")
print(len(pdf_eval))

# Save to DuckDB
duckdb_path = '/data/trusted/databases/trusted_data.db'
conn = duckdb.connect(duckdb_path)

try:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trusted_evaluation (
            evaluation_id INTEGER,
            student_id TEXT,
            course_code TEXT,
            evaluation TEXT,
            weight FLOAT,
            score FLOAT
        );
    """)
except:
    print('Table already exists')
    

# Optional: if you want to overwrite every time:
conn.execute("DELETE FROM trusted_evaluation")
# conn.execute("DELETE FROM metadata_catalog WHERE source_name = 'trusted_evaluations'")
conn.execute("INSERT INTO trusted_evaluation SELECT * FROM pdf_eval")

conn.close()

# Save metadata
metadata = {
    "source_name": "trusted_evaluation",
    "batch_id": latest_batch_path.split('batch=')[-1],  # Extracting batch ID from the path
    "ingestion_timestamp": datetime.now().isoformat(),
    "ingestion_date": datetime.now().strftime('%Y-%m-%d'),
    "record_count": len(pdf_eval),
    "temporal_path": latest_batch_path,
    "trusted_zone_path": duckdb_path,
    "persistent_path": "",  # Empty initially
    "process_status": "CLEANED_AND_SAVED_TO_TRUSTED",
    "error_message": None,
    "promotion_timestamp": None,
    "error_timestamp": None
}
try:
    add_metadata_entry(metadata)
except:
    print('batch ingested')

spark.stop()

