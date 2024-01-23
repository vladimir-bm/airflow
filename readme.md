# Airflow test work

Basic airflow integrations example

This repository contain docker compose configuration to run Airflow + Greenplum + Postgres and few DAGs

DAGs:
- 'create_db_users': create users in both db once airflow start first time
 - 'csv_integration':  read employee data from csv and sore data in greenplum + create scd2 transformation
 - 'api_integration': read first 500 wiki page titles using api nd store to greenplum
 - 'db_integration': transfer employee data from greenplum to postgres

# How to use
Clone repo
```bash
git clone https://github.com/vladimir-bm/airflow

cd airflow
```

_For linux users only:_
```bash
./create_env.sh
```

Start docker compose
```bash
docker compose up
```

Go to http://127.0.0.1:8080 and log in user/password - airflow/airflow