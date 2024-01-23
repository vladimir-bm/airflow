from datetime import datetime

from airflow import DAG
from airflow.operators.generic_transfer import GenericTransfer
from airflow.operators.postgres_operator import PostgresOperator


with DAG(
    dag_id='db_integration',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    prepare = PostgresOperator(
        task_id='prepare',
        postgres_conn_id='pgdb',
        sql=['sql/dml_employee_pg.sql', 'sql/prepare_employee.sql', 'sql/prepare_employee_hist.sql']
    )

    transfer_employee = GenericTransfer(
        task_id='transfer_employee',
        source_conn_id='gpdb',
        destination_conn_id='pgdb',
        sql='sql/transfer_employee.sql',
        destination_table='t_employee'
    )

    transfer_employee_hist = GenericTransfer(
        task_id='transfer_employee_hist',
        source_conn_id='gpdb',
        destination_conn_id='pgdb',
        sql='sql/transfer_employee_hist.sql',
        destination_table='t_employee_hist'
    )

    prepare >> transfer_employee >> transfer_employee_hist
