import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def cleanup():
    for file in os.listdir("./store_files_airflow"):
        if file.startswith("raw_store_transactions"):
            current_name = os.path.join("./store_files_airflow", file)
            new_name = os.path.join("./store_files_airflow", "raw_store_transactions.csv")
            os.rename(current_name, new_name)
        else:
            os.remove(os.path.join("./store_files_airflow", file))
        
    

default_args = {
    "owner": "LMP",
    "retries": 2,
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    dag_id="cleanup_store_dag",
    description="DAG to cleanup after running store_dag",
    start_date=datetime(2023, 9, 14),
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag:
    PythonOperator(
        task_id="cleanup_files",
        python_callable=cleanup
    )