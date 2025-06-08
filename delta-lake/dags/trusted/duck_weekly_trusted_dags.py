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
    "spark.sql.shuffle.partitions": "1",
    "spark.default.parallelism": "1",
    "spark.cores.max": "1",

    "spark.driver.memory": "512m",
}


with DAG(dag_id="duck_weekly3_trusted",
         tags=["weekly", "student information system"],
         default_args=default_args,
        #  schedule="@weekly",
         schedule=None,
         catchup=False) as dag:
    
    task_trusted_student = SparkSubmitOperator(
        task_id="task_trusted_student",
        application="/opt/airflow/spark/trusted/ingest/trusted_student.py",
        conn_id="spark-default",
        application_args=[],
    )

    task_trusted_faculty = SparkSubmitOperator(
        task_id="task_trusted_faculty",
        application="/opt/airflow/spark/trusted/ingest/trusted_faculty.py",
        conn_id="spark-default",
        application_args=[],
    )

    task_trusted_enrollment = SparkSubmitOperator(
        task_id="task_trusted_enrollment",
        application="/opt/airflow/spark/trusted/ingest/trusted_enrollment.py",
        conn_id="spark-default",
        application_args=[],
    )

    task_trusted_student >> task_trusted_faculty >> task_trusted_enrollment
    # task_trusted_enrollment 