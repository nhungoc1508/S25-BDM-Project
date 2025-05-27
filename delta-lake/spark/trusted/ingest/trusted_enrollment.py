import os
import glob
import duckdb
import pandas as pd
from pyspark.sql import SparkSession
from preprocess.preprocess_enrollment import preprocessing_pipeline
from metadata_manager import add_metadata_entry  # Assuming same metadata system is used
from datetime import datetime

# Spark Session
spark = SparkSession.builder.appName("EnrollmentProcessing").getOrCreate()

# Path config
landing_path = '/data/landing/sis_enrollment/'

# # Find the latest batch folder
all_batches = glob.glob(os.path.join(landing_path, 'date=*', 'batch=*'))
latest_batch_path = max(all_batches, key=os.path.getmtime)

# Read the data
abs_path = os.path.abspath(latest_batch_path)
df = spark.read.format("parquet").load(f'file://{abs_path}')

#Convert to Pandas and clean
df = df.withColumn("is_taking", df["is_taking"].cast("string"))
pdf = df.toPandas()
pdf = preprocessing_pipeline(pdf)

# pdf.to_csv('students.csv', index=False)

# Save to DuckDB
duckdb_path = 'spark/trusted/databases/trusted_data.db'
conn = duckdb.connect(duckdb_path)



try:
    conn.execute("""
        CREATE TABLE trusted_enrollment (
        student_id VARCHAR(20) NOT NULL,
        course_code VARCHAR(20) NOT NULL ,
        semester VARCHAR(50) NOT NULL,
        is_taking BOOLEAN NOT NULL,
        grade DECIMAL(5,2) CHECK (grade >= 0),
        PRIMARY KEY (student_id, course_code, semester),
        FOREIGN KEY (student_id) REFERENCES trusted_student(student_id) ,
        FOREIGN KEY (course_code) REFERENCES trusted_course(course_code)
    );
    """)
except Exception as e:
    print(e)


# Optional: if you want to overwrite every time:
conn.execute("DELETE FROM trusted_enrollment")
# conn.execute("DELETE FROM metadata_catalog WHERE source_name = 'trusted_faculties'")
conn.execute("INSERT INTO trusted_enrollment SELECT * FROM pdf")

re=conn.execute("SELECT * FROM trusted_enrollment")
print(re.fetchall())
conn.close()

# Save metadata
metadata = {
    "source_name": "trusted_enrollment",
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
add_metadata_entry(metadata)

spark.stop()
