import airflow
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

args = {
    "owner": "LMP",
    "start_date": airflow.utils.dates.days_ago(1)
}

def push_function(**kwargs):
    message = "Pushed message from task 1"
    # ti = task instance
    task_instance = kwargs["ti"]
    task_instance.xcom_push(key="message", value=message)

def pull_function(**kwargs):
    task_instance = kwargs["ti"]
    pulled_message = task_instance.xcom_pull(key="message")
    print(f"Pulled message {pulled_message}")
    task_2_message = "A message from task 2"
    task_instance.xcom_push(key="task_2_message", value=task_2_message)

    
def null_function():
    print("This task does nothing")


def skip_function(**kwargs):
    task_instance = kwargs["ti"]
    task_1_message = task_instance.xcom_pull(key="message")
    task_2_message = task_instance.xcom_pull(key="task_2_message")
    
    print(f"Task 1 message: {task_1_message}")
    print(f"Task 2 message: {task_2_message}")
    
    return {"detail": "Message from return statement"} # this will be available via XCom

def last_task(**kwargs):
    task_instance = kwargs["ti"]
    pulled = task_instance.xcom_pull() # no key
    print(pulled)


dag = DAG(
    dag_id="xcom_dag",
    default_args=args,
    schedule_interval=None
)

with dag:
    t1 = PythonOperator(
        task_id="push_task",
        python_callable=push_function,
        provide_context=True,
    )
    
    t2 = PythonOperator(
        task_id="pull_task",
        python_callable=pull_function,
        provide_context=True
    )
    
    t3 = PythonOperator(
        task_id="null_task",
        python_callable=null_function,
        # provide_context=True -> when this is True, the function requires **kwargs
    )

    t4 = PythonOperator(
        task_id="skip_xcom_task",
        python_callable=skip_function,
        provide_context=True
    )
    
    t5 = PythonOperator(
        task_id="last_task",
        python_callable=last_task,
        provide_context=True
    )
    
    t1 >> t2 >> t3 >> t4 >> t5