from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 4, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(dag_id="metadata_summary",
         tags=["organization"],
         default_args=default_args,
        #  schedule="@weekly",
         schedule=None,
         catchup=False) as dag:
    
    task_summarize_metadata = BashOperator(
        task_id="run_task_summarize_metadata",
        bash_command="python /opt/airflow/spark/landing/summarize_metadata.py"
    )

    task_summarize_metadata