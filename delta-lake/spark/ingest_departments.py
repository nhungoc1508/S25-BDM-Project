from pyspark.sql import SparkSession
from delta import *
from delta.tables import DeltaTable

def ingest_departments_data():
    # Create Spark session
    builder = SparkSession.builder \
        .appName("DepartmentsIngestion") \
        .config("spark.driver.host", "delta-lake-airflow-worker-1") \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Read data from PostgreSQL (departments table)
    df = spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_sis:5432/university")\
        .option("dbtable", "departments")\
        .option("user", "admin") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .load()

    # Write to Delta Lake (temporal zone)
    delta_path = "file:///data/landing/temporal/departments"
    df.write.format("delta").mode("overwrite").save(delta_path)
    print("Delta write succeeded!")

    # Stop the Spark session after processing
    spark.sparkContext.stop()

if __name__ == '__main__':
    ingest_departments_data()