# from config import spark_configs
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

spark_configs = {
    "spark.master": "spark://spark-master:7077",
    "spark.driver.extraClassPath": "/opt/bitnami/spark/jars/postgresql-42.6.0.jar:/opt/bitnami/spark/jars/delta-spark_2.12-3.0.0.jar:/opt/bitnami/spark/jars/delta-storage-3.0.0.jar",
    "spark.executor.extraClassPath": "/opt/bitnami/spark/jars/postgresql-42.6.0.jar:/opt/bitnami/spark/jars/delta-spark_2.12-3.0.0.jar:/opt/bitnami/spark/jars/delta-storage-3.0.0.jar",
    "spark.jars": "/opt/airflow/spark/jars/postgresql-42.6.0.jar,/opt/bitnami/spark/jars/delta-spark_2.12-3.0.0.jar,/opt/bitnami/spark/jars/delta-storage-3.0.0.jar",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",

    "spark.submit.deployMode": "client",
    "spark.driver.host": "airflow-worker",
    "spark.dynamicAllocation.enabled": "false",
    "spark.executor.instances": "1",
    "spark.executor.cores": "1",
    "spark.executor.memory": "512m",
    "spark.memory.fraction": "0.5",
    "spark.memory.storageFraction": "0.3",
    "spark.executor.memoryOverhead": "512m",

    "spark.speculation": "true",
    "spark.sql.shuffle.partitions": "2",
    "spark.default.parallelism": "2",

    "spark.driver.memory": "512m",
}

with DAG(dag_id="trusted_evaluation",
        tags=['trusted', 'evaluation'],
        default_args=default_args,
        schedule=None,
        catchup=False) as dag:
    task_trusted_evaluation = SparkSubmitOperator(
        task_id="task_trusted_evaluation",
        application="/opt/airflow/spark/trusted/ingest/trusted_evaluation.py",
        conn_id="spark-default",
        application_args=[],
    )

    task_trusted_evaluation