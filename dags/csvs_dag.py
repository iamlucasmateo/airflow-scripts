from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd

def _first_task():
    df = pd.DataFrame({
        "col1": [1, 2, 5, 8, 4],
        "col2": [76, 3, 567, 23.121, 23],
        "col3": ["info1", "info2", "info3", "info4", "info5"] 
    })
    df.to_csv("download.csv", index=False)

def _second_task():
    df = pd.read_csv("download.csv")
    df["col4"] = df["col1"] * df["col2"]
    df.to_csv("upload.csv", index=False)
    
default_args = {
    "owner": "LMP",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="csvs_dag",
    description="DAG to download and upload csvs",
    start_date=datetime(2023, 9, 14),
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag:
    first_task = PythonOperator(
        task_id="first_task",
        python_callable=_first_task
    )

    second_task = PythonOperator(
        task_id="second_task",
        python_callable=_second_task
    )
    first_task >> second_task
