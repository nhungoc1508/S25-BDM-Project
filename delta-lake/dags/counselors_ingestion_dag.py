from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 31),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
with DAG(dag_id="counselors_ingestion",
         tags=["ingestion", "counseling"],
         default_args=default_args,
         schedule="@weekly",
         catchup=False) as dag:

    ingest_task = SparkSubmitOperator(
        task_id="submit_counselors_job",
        application="/opt/airflow/spark/ingest_counselors.py",
        conn_id="spark-default",
        application_args=[],
        packages="io.delta:delta-core_2.12:2.2.0",
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.driver.extraClassPath": "/opt/bitnami/spark/jars/postgresql-42.6.0.jar",
            "spark.executor.extraClassPath": "/opt/bitnami/spark/jars/postgresql-42.6.0.jar",
            "spark.jars": "/opt/bitnami/spark/jars/postgresql-42.6.0.jar",
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",

            "spark.submit.deployMode": "client",
            "spark.dynamicAllocation.enabled": "false",
            "spark.executor.instances": "2",
            "spark.executor.cores": "1",
            "spark.executor.memory": "1g",
            "spark.memory.fraction": "0.5",
            "spark.memory.storageFraction": "0.3",
            "spark.executor.memoryOverhead": "512m",

            "spark.speculation": "true",
            "spark.sql.shuffle.partitions": "2",
            "spark.default.parallelism": "2",

            "spark.driver.memory": "1g",
        }
    )

    ingest_task