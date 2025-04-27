from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 4, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

spark_configs = {
    "spark.master": "spark://spark-master:7077",
    "spark.driver.extraClassPath": "/opt/bitnami/spark/jars/postgresql-42.6.0.jar",
    "spark.executor.extraClassPath": "/opt/bitnami/spark/jars/postgresql-42.6.0.jar",
    "spark.jars": "/opt/airflow/spark/jars/postgresql-42.6.0.jar",
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

    "spark.driver.memory": "512m",
}

with DAG(dag_id="financial_files_ingestion",
         tags=["ingestion", "finance"],
         default_args=default_args,
         schedule="@daily",
         catchup=False) as dag:
    
    task_ingest_financial_files = SparkSubmitOperator(
        task_id="submit_task_ingest_financial_files",
        application="/opt/airflow/spark/ingest_financial_files.py",
        conn_id="spark-default",
        application_args=[],
        packages="io.delta:delta-core_2.12:2.2.0",
        conf=spark_configs
    )

    task_ingest_financial_files