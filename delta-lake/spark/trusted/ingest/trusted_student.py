import os
import glob
import duckdb
import pandas as pd
from pyspark.sql import SparkSession
from preprocess.preprocess_students import preprocessing_pipeline
from metadata_manager import add_metadata_entry  # Assuming same metadata system is used
from datetime import datetime

# Spark Session
spark = SparkSession.builder.appName("StudentsProcessing").getOrCreate()

# Path config
landing_path = '/data/landing/sis_students/'

# # Find the latest batch folder
all_batches = glob.glob(os.path.join(landing_path, 'date=*', 'batch=*'))
latest_batch_path = max(all_batches, key=os.path.getmtime)

# Read the data
abs_path = os.path.abspath(latest_batch_path)
df = spark.read.format("parquet").load(f'file://{abs_path}')

# Convert to Pandas and clean
pdf = df.toPandas()
pdf = preprocessing_pipeline(pdf)

print(len(pdf))
print(pdf.head())
# pdf.to_csv('students.csv', index=False)

# Save to DuckDB
duckdb_path = '/data/trusted/databases/trusted_data.db'
conn = duckdb.connect(duckdb_path)


try:
    conn.execute("""
       CREATE TABLE trusted_student (
        student_id VARCHAR(20) PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        phone VARCHAR(20),
        address TEXT,
        gender VARCHAR(10) CHECK (gender IN ('male', 'female', 'other')) ,
        email VARCHAR(255) UNIQUE NOT NULL
    );
    """)
except:
    print('Table already exists')


# Optional: if you want to overwrite every time:
conn.execute("DELETE FROM trusted_student")
conn.execute("INSERT INTO trusted_student SELECT * FROM pdf")

conn.close()

# Save metadata
metadata = {
    "source_name": "trusted_student",
    "batch_id": latest_batch_path.split('batch=')[-1],  # Extracting batch ID from the path
    "ingestion_timestamp": datetime.now().isoformat(),
    "ingestion_date": datetime.now().strftime('%Y-%m-%d'),
    "record_count": len(pdf),
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