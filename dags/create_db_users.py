from datetime import datetime

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator


def create(connection_id):
    pg_hook = PostgresHook(postgres_conn_id=connection_id, schema='postgres')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    conn.autocommit = True
    cursor.execute("create database datastore")
    cursor.execute("create user datastore with password 'datastore'")
    cursor.execute("grant all privileges on database datastore to datastore")
    cursor.close()
    conn.close()

with DAG(
    dag_id='create_db_users',
    start_date=datetime(2024, 1, 1),
    schedule='@once',
    catchup=False
) as dag:

    first = PythonOperator(
        task_id='create_gp_user',
        provide_context=True,
        python_callable=create,
        op_kwargs={"connection_id": 'GPDB_ADMIN'}
    )

    second = PythonOperator(
        task_id='create_pg_user',
        provide_context=True,
        python_callable=create,
        op_kwargs={"connection_id": 'PGDB_ADMIN'}
    )

    first >> second
