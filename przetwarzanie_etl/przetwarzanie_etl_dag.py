from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from datetime import datetime


PROJECT_ID = "przetwarzanie-etl"     
BUCKET_NAME = "bucket__etl" 
CLUSTER_NAME = "cluster-etl"   
REGION = "us-central1"

TRANSFORM_SCRIPT = f"gs://{BUCKET_NAME}/scripts/1_transform.py"
LOAD_SCRIPT = f"gs://{BUCKET_NAME}/scripts/2_load.py"

SCENARIOS = {
    "small": {
        "schedule": None,
        "filter": "WHERE trip_start_timestamp >= '2022-01-01' AND trip_start_timestamp < '2023-01-01'"
    },
    "medium": {
        "schedule": None,
        "filter": "WHERE trip_start_timestamp >= '2016-01-01' AND trip_start_timestamp < '2018-01-01'"
    },
    "large": {
        "schedule": None,
        "filter": "WHERE trip_start_timestamp >= '2013-01-01' AND trip_start_timestamp < '2016-01-01'"
    }
}

for size, config in SCENARIOS.items():
    dag_id = f"przetwarzanie_etl_{size}"

    default_args = {
        'owner': 'polejarczyk_student',
        'start_date': datetime(2023, 1, 1),
        'catchup': False
    }

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=config['schedule'],
        tags=['etl', size]
    )

    with dag:
        extract_sql = f"""
        EXPORT DATA OPTIONS(
          uri='gs://{BUCKET_NAME}/data/raw/{size}/taxi_*.parquet',
          format='PARQUET',
          overwrite=true
        ) AS
        SELECT * FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
        {config['filter']}
        """

        t1_extract = BigQueryInsertJobOperator(
            task_id="Extract",
            configuration={"query": {"query": extract_sql, "useLegacySql": False}},
            location='US',
            project_id=PROJECT_ID
        )

        t2_transform = DataprocSubmitJobOperator(
            task_id="Transform",
            job={
                "placement": {"cluster_name": CLUSTER_NAME},
                "pyspark_job": {
                    "main_python_file_uri": TRANSFORM_SCRIPT,
                    "args": [size]
                },
            },
            region=REGION,
            project_id=PROJECT_ID
        )

        t3_load = DataprocSubmitJobOperator(
            task_id="Load",
            job={
                "placement": {"cluster_name": CLUSTER_NAME},
                "pyspark_job": {
                    "main_python_file_uri": LOAD_SCRIPT,
                    "args": [size]
                },
            },
            region=REGION,
            project_id=PROJECT_ID
        )

        t1_extract >> t2_transform >> t3_load

    globals()[dag_id] = dag