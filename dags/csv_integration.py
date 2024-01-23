import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
CSV_LOCATION = 'data/employee.csv'

def extract(ti=None):
    import pandas as pd
    csv_path = os.path.join(AIRFLOW_HOME, CSV_LOCATION)
    columns = ['department', 'rank', 'staff_number', 'full_name', 'birth_date', 'address', 'phone1', 'phone2', 'date_month', 'work_time']
    df = pd.read_csv(csv_path, delimiter=';', index_col=0, header=None, names=columns)
    ti.xcom_push(key="employee_df", value=df)

def load(ti):
    import pandas as pd
    data = ti.xcom_pull(key='employee_df', task_ids=['extract'])[0]
    df = pd.DataFrame(data)
    pg_hook = PostgresHook(postgres_conn_id='gpdb', schema='datastore')
    con = pg_hook.get_sqlalchemy_engine()
    df.to_sql('t_employee', con=con, if_exists='append', chunksize=1000)
    print('data saved to temp table')

with DAG(
    dag_id='csv_integration',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    prepare = PostgresOperator(
        task_id='prepare',
        postgres_conn_id='gpdb',
        sql=['sql/dml_employee_gp.sql', 'sql/prepare_employee.sql']
    )

    extract = PythonOperator(
        task_id='extract',
        python_callable=extract,
        do_xcom_push=True,
    )

    load = PythonOperator(
        task_id='load',
        python_callable=load,
    )

    transform = PostgresOperator(
        task_id='transform',
        postgres_conn_id='gpdb',
        sql=['sql/transform_employee.sql']
    )

    prepare >> extract >> load >> transform
