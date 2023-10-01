from datetime import datetime

from airflow import DAG
from airflow.utils import dates
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator


dag = DAG(
    dag_id="airflow_vars",
    schedule_interval=None,
    default_args={
        "start_date": dates.days_ago(1)
    }
)


def using_var():
    print(Variable.get("app_scoped_variable"))

def task2():
    print("Running task 2")

def task3():
    print("Running task 3")

def task4():
    print("Running task 4")


with dag:
    t1 = PythonOperator(
        task_id="using_var",
        python_callable=using_var
    )
    
    t2 = PythonOperator(
        task_id="task_2",
        python_callable=task2
    )

    t3 = PythonOperator(
        task_id="task_3",
        python_callable=task3
    )

    t4 = PythonOperator(
        task_id="task_4",
        python_callable=task4
    )
    
    td = DummyOperator(task_id="dummy_task")
    
    # without the DummyOperator this error would be raised: 
    # Broken DAG: [/usr/local/airflow/dags/airflow_var.py] unsupported operand type(s) for >>: 'list' and 'list'
    [t1, t2] >> td >> [t3, t4]