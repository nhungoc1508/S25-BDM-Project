# from config import spark_configs
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 27),
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

with DAG(
    dag_id='trusted_fact_sis',
    tags=["processing", "trusted", "student information system"],
    default_args=default_args,
    schedule=None,
    catchup=False
) as dag:    

    task_dropouts = SparkSubmitOperator(
        task_id='trusted_dropouts',
        application='/opt/airflow/spark/trusted/ingest/trusted_dropouts.py',
        conn_id='spark-default',
        conf=spark_configs
    )

    task_enrollment = SparkSubmitOperator(
        task_id='trusted_enrollment',
        application='/opt/airflow/spark/trusted/ingest/trusted_enrollment.py',
        conn_id='spark-default',
        conf=spark_configs
    )

    task_evaluation = SparkSubmitOperator(
        task_id='trusted_evaluation',
        application='/opt/airflow/spark/trusted/ingest/trusted_evaluation.py',
        conn_id='spark-default',
        conf=spark_configs
    )    

    task_graduations = SparkSubmitOperator(
        task_id='trusted_graduations',
        application='/opt/airflow/spark/trusted/ingest/trusted_graduations.py',
        conn_id='spark-default',
        conf=spark_configs
    )

    task_leave_of_absence = SparkSubmitOperator(
        task_id='trusted_leave_of_absence',
        application='/opt/airflow/spark/trusted/ingest/trusted_leave_of_absence.py',
        conn_id='spark-default',
        conf=spark_configs
    )

    task_student_enrollment = SparkSubmitOperator(
        task_id='trusted_student_enrollment',
        application='/opt/airflow/spark/trusted/ingest/trusted_student_enrollment.py',
        conn_id='spark-default',
        conf=spark_configs
    )

            
    task_enrollment >> task_evaluation >> task_dropouts >> task_graduations >> task_leave_of_absence >> task_student_enrollment

    
    