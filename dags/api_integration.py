import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
API_URL = 'https://ru.wikipedia.org/w/api.php?action=query&list=allpages&aplimit=500&apfrom={}&format=json'
API_ARGUMENT_VALUE = 'A'


def extract(ti=None):
    import json
    import pandas as pd
    import requests
    api_query_value = API_ARGUMENT_VALUE
    url = API_URL.format(api_query_value)
    ret = requests.get(url).content
    data = json.loads(ret)
    print(data)
    df = None
    if data and 'query' in data and 'allpages' in data['query']:
        df = pd.DataFrame.from_records(data['query']['allpages'])
    ti.xcom_push(key="wiki_df", value=df)


def load(ti):
    import pandas as pd
    data = ti.xcom_pull(key='wiki_df', task_ids=['extract'])
    df = pd.DataFrame(data[0])
    print(type(df), df)
    pg_hook = PostgresHook(postgres_conn_id='gpdb', schema='datastore')
    con = pg_hook.get_sqlalchemy_engine()
    # con = Connection(conn_id='gpdb_default')
    df.to_sql('t_wiki_pages', con=con, if_exists='append', chunksize=1000, index=False)
    print('data saved to temp table')


with DAG(
    dag_id='api_integration',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    prepare = PostgresOperator(
        task_id='prepare',
        postgres_conn_id='gpdb',
        sql=['sql/dml_wiki.sql', 'sql/prepare_wiki.sql']
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

    prepare >> extract >> load
