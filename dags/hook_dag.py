from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import execute_values


default_args = {
    "owner": "LMP",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2019, 11, 26)
}

dag = DAG("hooks_demo", default_args=default_args, schedule_interval=None)


def transfer_function(ds, **kwargs):
    query = "SELECT * FROM source_city_table"
    source_hook = MySqlHook(mysql_conn_id="mysql_store_connection")
    source_conn = source_hook.get_conn()
    
    destination_hook = PostgresHook(postgres_conn_id="postgres_hook", schema="airflow")
    destination_conn = destination_hook.get_conn()
    
    source_cursor = source_conn.cursor()
    destination_cursor = destination_conn.cursor()
    
    source_cursor.execute(query)
    
    records = source_cursor.fetchall()
    
    if records:
        execute_values(destination_cursor, "INSERT INTO destination_city_table VALUES %s", records)
        destination_conn.commit()
    
    source_cursor.close()
    destination_cursor.close()
    source_conn.close()
    destination_conn.close()
    print("Data transferred successfully")
    
    

with dag:
    t1 = MySqlOperator(
        task_id="create_hook_example_table",
        mysql_conn_id="mysql_store_connection",
        sql=r"CREATE TABLE IF NOT EXISTS source_city_table(city_name VARCHAR(50), city_code VARCHAR(20));",
    )

    t2 = MySqlOperator(
        task_id="delete_data_from_hook_example_table",
        mysql_conn_id="mysql_store_connection",
        sql=r"DELETE FROM source_city_table"
    )

    t3 = MySqlOperator(
        task_id="populate_hook_example_table",
        mysql_conn_id="mysql_store_connection",
        sql=r"""
            INSERT INTO source_city_table (city_name, city_code)
            VALUES 
                ('New York', 'ny'),
                ('San Francisco', 'sf'),
                ('Buenos Aires', 'ba'),
                ('Mexico City', 'cdmx'),
                ('Sao Paulo', 'sp')
            ;
        """,
    )

    t4 = PostgresOperator(
        task_id="create_destination_table",
        postgres_conn_id="postgres_hook",
        sql=r"CREATE TABLE IF NOT EXISTS destination_city_table (city_name VARCHAR(50), city_code VARCHAR(20));",
    )
    
    t5 = PythonOperator(
        task_id="transfer_table",
        python_callable=transfer_function,
        provide_context=True
    )
    
    t1 >> t2 >> t3 >> t4 >> t5
