from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.email_operator import EmailOperator
from airflow.contrib.sensors.file_sensor import FileSensor

from processing.store_data_cleaner import clean_store_data


default_args = {
    "owner": "LMP",
    "retries": 1,
    "start_date": datetime(2023, 9, 14),
    "retry_delay": timedelta(minutes=1),
}


def check_condition(**kwargs):
    import os
    send_email = os.getenv("SEND_EMAIL")
    print("SEND", send_email)
    
    return send_email

    
def branch(**kwargs):
    task_instance = kwargs["ti"]
    send_email: bool = task_instance.xcom_pull()
    print("SEND received", send_email)
    
    return (
        "log_task_completed" if not send_email else "log_task_completed"
    )
    


dag = DAG(
    "store_dag",
    default_args=default_args,
    schedule_interval=None, #@daily
    catchup=False,
    template_searchpath=["/usr/local/airflow/sql_files"]
)

t1 = FileSensor(
    task_id="check_file_exists",
    filepath="/usr/local/airflow/store_files_airflow/raw_store_transactions.csv",
    fs_conn_id="fs_default",
    poke_interval=10,
    timeout=150,
    soft_fail=True,
    dag=dag
)

# t1 = BashOperator(
#     task_id="check_file_exists",
#     bash_command="shasum ~/store_files_airflow/raw_store_transactions.csv",
#     retries=2,
#     retry_delay=timedelta(seconds=15),
#     dag=dag
# )

t2 = PythonOperator(
    task_id="clean_raw_csv",
    python_callable=clean_store_data,
    dag=dag
)

t3 = MySqlOperator(
    task_id="create_mysql_table",
    mysql_conn_id="mysql_store_connection",
    sql="create_store_table.sql",
    dag=dag
)

t4 = MySqlOperator(
    task_id="insert_into_table",
    mysql_conn_id="mysql_store_connection",
    sql="insert_store_data.sql",
    dag=dag
)


t5 = MySqlOperator(
    task_id="select_from_table",
    mysql_conn_id="mysql_store_connection",
    sql="select_store_data.sql",
    dag=dag
)

yesterday_date = datetime.strftime(datetime.now() - timedelta(1), "%Y-%m-%d")

move_file_1 = BashOperator(
    task_id="move_file_1",
    bash_command=(
        "cat ~/store_files_airflow/location_wise_profit.csv"
        "&& mv ~/store_files_airflow/location_wise_profit.csv ~/store_files_airflow/location_wise_profit_%s.csv" % yesterday_date
    ),
    dag=dag
)

move_file_2 = BashOperator(
    task_id="move_file_2",
    bash_command=(
        "cat ~/store_files_airflow/store_wise_profit.csv"
        "&& mv ~/store_files_airflow/store_wise_profit.csv ~/store_files_airflow/store_wise_profit_%s.csv" % yesterday_date
    ),
    dag=dag
)

check_condition_task = PythonOperator(
    task_id="check_condition",
    python_callable=check_condition,
    dag=dag
    
)

branch_task = BranchPythonOperator(
    task_id="branch_send_email_or_log",
    python_callable=branch,
    provide_context=True,
    dag=dag
)

email_task = EmailOperator(
    task_id="send_email",
    to="lucasmateo300@gmail.com",
    subject="Daily report generated",
    html_content="<h1>Congratulations! Your store reports are ready.</h1>",
    files=[
        "/usr/local/airflow/store_files_airflow/location_wise_profit_%s.csv" % yesterday_date,
        "/usr/local/airflow/store_files_airflow/store_wise_profit_%s.csv" % yesterday_date
    ],
    dag=dag
)

log_task = BashOperator(
    task_id="log_task_completed",
    bash_command=(
        'echo "Operations have been completed" | tee -a ~/store_files_airflow/log_file.txt'
    ),
    dag=dag
)

dummy_task = DummyOperator(
    task_id="dummy_store_task",
    trigger_rule="one_success", # none_failed_min_one_success is the current version
    dag=dag
)

rename_files = BashOperator(
    task_id="rename_files",
    bash_command=(
        "mv ~/store_files_airflow/raw_store_transactions.csv ~/store_files_airflow/raw_store_transactions_%s.csv" % yesterday_date
    ),
    dag=dag
)

delete_log = BashOperator(
    task_id="delete_log",
    bash_command=(
        "rm -f ~/store_files_airflow/log_file.txt"
    ),
    dag=dag
)



t1 >> t2 >> t3 >> t4 >> t5 >> [move_file_1, move_file_2] >> check_condition_task >> branch_task >> [email_task, log_task] >> dummy_task >> [rename_files, delete_log]