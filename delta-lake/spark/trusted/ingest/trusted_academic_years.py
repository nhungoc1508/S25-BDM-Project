import os
import glob
import duckdb
import pandas as pd
from pyspark.sql import SparkSession
from preprocess.preprocess_academic_years import preprocessing_pipeline
from metadata_manager import add_metadata_entry  # Assuming same metadata system is used
from datetime import datetime

# Spark Session
spark = SparkSession.builder.appName("AcademicYearsProcessing").getOrCreate()

# Path config
landing_path = '/data/landing/sis_academic_years/'

# # Find the latest batch folder
all_batches = glob.glob(os.path.join(landing_path, 'date=*', 'batch=*'))
latest_batch_path = max(all_batches, key=os.path.getmtime)

# Read the data
abs_path = os.path.abspath(latest_batch_path)
df = spark.read.format("parquet").load(f'file://{abs_path}')
df.printSchema() 
# Convert to Pandas and clean
pdf = df.toPandas()
pdf = preprocessing_pipeline(pdf)

print(len(pdf))
print(pdf.head())
# pdf.to_csv('courses.csv', index=False)

# Save to DuckDB
duckdb_path = '/data/trusted/databases/trusted_data.db'
conn = duckdb.connect(duckdb_path)

try:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trusted_academic_years (
            academic_year_id VARCHAR,
            start_year INT,
            end_year INT,
            start_date DATE,
            end_date DATE
        );
    """)
except Exception as e:
    print("Could not create table:", e)

# Optional: overwrite data
conn.execute("DELETE FROM trusted_academic_years")
conn.execute("INSERT INTO trusted_academic_years SELECT * FROM pdf")

# Validate
# print(conn.execute("SELECT COUNT(*) FROM trusted_academic_years").fetchone())

# Close connection
conn.close()

# Save metadata
metadata = {
    "source_name": "trusted_academic_years",
    "batch_id": latest_batch_path.split('batch=')[-1],
    "ingestion_timestamp": datetime.now().isoformat(),
    "ingestion_date": datetime.now().strftime('%Y-%m-%d'),
    "record_count": len(pdf),
    "temporal_path": latest_batch_path,
    "trusted_zone_path": duckdb_path,
    "persistent_path": "",
    "process_status": "CLEANED_AND_SAVED_TO_TRUSTED",
    "error_message": None,
    "promotion_timestamp": None,
    "error_timestamp": None
}
try:
    add_metadata_entry(metadata)
except Exception as e:
    print("Metadata logging failed:", e)

spark.stop()