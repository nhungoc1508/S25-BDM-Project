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


with DAG(dag_id="trusted_dimension_sis",
         tags=["processing", "trusted", "student information system"],
         default_args=default_args,
        #  schedule="@monthly",
         schedule=None,
         catchup=False) as dag:
    
    task_academic_years = SparkSubmitOperator(
        task_id='trusted_academic_years',
        application='/opt/airflow/spark/trusted/ingest/trusted_academic_years.py',
        conn_id='spark-default',
        conf=spark_configs
    )


    task_trusted_course = SparkSubmitOperator(
        task_id="task_trusted_course",
        application="/opt/airflow/spark/trusted/ingest/trusted_course.py",
        conn_id="spark-default",
        application_args=[],
    )

    task_degree_names = SparkSubmitOperator(
        task_id='trusted_degree_names',
        application='/opt/airflow/spark/trusted/ingest/trusted_degree_names.py',
        conn_id='spark-default',
        conf=spark_configs
    )

    task_trusted_department = SparkSubmitOperator(
        task_id="task_trusted_department",
        application="/opt/airflow/spark/trusted/ingest/trusted_department.py",
        conn_id="spark-default",
        application_args=[],
    )

    task_trusted_faculty = SparkSubmitOperator(
        task_id="task_trusted_faculty",
        application="/opt/airflow/spark/trusted/ingest/trusted_faculty.py",
        conn_id="spark-default",
        application_args=[],
    )


    task_trusted_student = SparkSubmitOperator(
        task_id="task_trusted_student",
        application="/opt/airflow/spark/trusted/ingest/trusted_student.py",
        conn_id="spark-default",
        application_args=[],
    )


    task_academic_years >> task_degree_names >> task_trusted_department >> task_trusted_course >> task_trusted_faculty >> task_trusted_student