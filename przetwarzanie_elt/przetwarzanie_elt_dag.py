#przetwarzanie_elt_dag.py

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

PROJECT_ID = "przetwarzanie-elt"     
DATASET_RAW = "raw"             

AIRFLOW_HOME = "/home/airflow/gcs/dags"
DBT_DIR = f"{AIRFLOW_HOME}/dbt/przetwarzanie_elt"

SCENARIOS = {
    "small":  "WHERE trip_start_timestamp >= '2022-01-01' AND trip_start_timestamp < '2023-01-01'",
    "medium": "WHERE trip_start_timestamp >= '2016-01-01' AND trip_start_timestamp < '2018-01-01'",
    "large":  "WHERE trip_start_timestamp >= '2013-01-01' AND trip_start_timestamp < '2016-01-01'"
}

for size, filter_sql in SCENARIOS.items():
    dag_id = f"przetwarzanie_elt_{size}"

    dag = DAG(
        dag_id=dag_id,
        default_args={'owner': 'polejarczyk_student', 'start_date': datetime(2023, 1, 1)},
        schedule_interval=None,
        catchup=False,
        tags=['elt', size]
    )

    with dag:
        table_name = f"{PROJECT_ID}.{DATASET_RAW}.taxi_trips_{size}"
        
        load_sql = f"""
        CREATE OR REPLACE TABLE `{table_name}` AS
        SELECT *
        FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
        {filter_sql};
        """

        t1_extract_load = BigQueryInsertJobOperator(
            task_id="Extract-Load",
            configuration={"query": {"query": load_sql, "useLegacySql": False}},
            location='US',
            project_id=PROJECT_ID
        )

        dbt_cmd = f"""
        cd {DBT_DIR} && \
        dbt build --profiles-dir . --target dev --vars '{{"source_size": "{size}"}}'
        """

        t2_dbt_transform = BashOperator(
            task_id="Transform",
            bash_command=dbt_cmd
        )

        t1_extract_load >> t2_dbt_transform

    globals()[dag_id] = dag