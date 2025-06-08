import os
import glob
import duckdb
import pandas as pd
from pyspark.sql import SparkSession
from preprocess.preprocess_faculty import preprocessing_pipeline
from metadata_manager import add_metadata_entry  # Assuming same metadata system is used
from datetime import datetime

# Spark Session
spark = SparkSession.builder.appName("FacultyProcessing").getOrCreate()

# Path config
landing_path = '/data/landing/sis_faculty/'

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
        CREATE TABLE trusted_faculty (
            instructor_code VARCHAR(20) PRIMARY KEY,
            first_name VARCHAR(50) NOT NULL,
            last_name VARCHAR(50) NOT NULL,
            dept_code VARCHAR(10) NOT NULL,
            phone VARCHAR(20),
            email VARCHAR(100) NOT NULL,
            office_number VARCHAR(20),
            building_code VARCHAR(10),
            status VARCHAR(50),
            terminal_degree VARCHAR(50),
            institution VARCHAR(100),
            --FOREIGN KEY (dept_code) REFERENCES trusted_department(dept_code) 
    );
    """)
except:
    print('Table already exists')


# Optional: if you want to overwrite every time:
conn.execute("DELETE FROM trusted_faculty")
# conn.execute("DELETE FROM metadata_catalog WHERE source_name = 'trusted_faculties'")
conn.execute("INSERT INTO trusted_faculty SELECT * FROM pdf")

re=conn.execute("SELECT * FROM trusted_faculty")
# print(re.fetchall())
conn.close()

# Save metadata
metadata = {
    "source_name": "trusted_faculty",
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
