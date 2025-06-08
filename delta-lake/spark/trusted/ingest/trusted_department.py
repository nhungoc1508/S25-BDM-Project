import os
import glob
import duckdb
import pandas as pd
from pyspark.sql import SparkSession
from preprocess.preprocess_department import preprocessing_pipeline
from metadata_manager import add_metadata_entry  # Assuming same metadata system is used
from datetime import datetime

# Spark Session
spark = SparkSession.builder.appName("DepartmentsProcessing").getOrCreate()
print("[DEBUG] 1")

# Path config
landing_path = '/data/landing/sis_departments/'

# # Find the latest batch folder
all_batches = glob.glob(os.path.join(landing_path, 'date=*', 'batch=*'))
latest_batch_path = max(all_batches, key=os.path.getmtime)

# Read the data
abs_path = os.path.abspath(latest_batch_path)
df = spark.read.format("parquet").load(f'file://{abs_path}')
print("[DEBUG] 2")

# Convert to Pandas and clean
pdf = df.toPandas()
pdf = preprocessing_pipeline(pdf)
print("[DEBUG] 3")

print(len(pdf))
print(pdf.head())
# pdf.to_csv('file:///data/trusted/departments.csv', index=False)

# Save to DuckDB
duckdb_path = '/data/trusted/databases/trusted_data.db'
conn = duckdb.connect(duckdb_path)

try:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trusted_department (
            dept_code TEXT PRIMARY KEY,
            division VARCHAR(50) NOT NULL,
            name VARCHAR(100) NOT NULL,
            office_number INT4,
            building_code VARCHAR(10),
            blurb TEXT
        );
    """)
except:
    print('Table already exists')


# Optional: if you want to overwrite every time:
conn.execute("DELETE FROM trusted_department")
# conn.execute("DELETE FROM metadata_catalog WHERE source_name = 'trusted_departments'")
conn.execute("INSERT INTO trusted_department SELECT * FROM pdf")

# re=conn.execute("SELECT * FROM trusted_department")
# print(re.fetchall())
# conn.close()

# Save metadata
metadata = {
    "source_name": "trusted_department",
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
except Exception as e:
    print(e)

spark.stop()
