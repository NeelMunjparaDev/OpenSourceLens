from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.standard.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
import pytz
from gh_archive_pipeline.scripts.ingest_hourly import main as ingest_main
from gh_archive_pipeline.transformation.bronze_to_silver import main as bronze_to_silver_main
from gh_archive_pipeline.transformation.silver_to_gold import main as silver_to_gold_main

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

ist = pytz.timezone('Asia/Kolkata')
start_date = datetime(2024, 1, 1, tzinfo=ist)

@dag(
    dag_id='github_pipeline',
    default_args=default_args,
    description='Ingest, transform, and aggregate GitHub data',
    schedule='@hourly',
    start_date=start_date,
    catchup=False,
    max_active_runs=1,
)
def github_pipeline_dag():
    @task(execution_timeout=timedelta(minutes=15))
    def ingest_local_data():
        ingest_main()

    sense_bronze = FileSensor(
        task_id='sense_bronze',
        fs_conn_id='fs_default',
        filepath='/Users/neel/Desktop/databricksProject/data_pipeline_project/local_data/bronze/*',
        poke_interval=10,
        timeout=300,
    )

    @task(execution_timeout=timedelta(minutes=20))
    def transform_bronze_to_silver():
        bronze_to_silver_main()

    sense_silver = FileSensor(
        task_id='sense_silver',
        fs_conn_id='fs_default',
        filepath='/Users/neel/Desktop/databricksProject/data_pipeline_project/local_data/datalake/github_commits/silver/metadata/*',
        poke_interval=10,
        timeout=300,
    )

    @task(execution_timeout=timedelta(minutes=20))
    def perform_aggregations():
        silver_to_gold_main()

    ingest = ingest_local_data()
    transform = transform_bronze_to_silver()
    aggregate = perform_aggregations()

    ingest >> sense_bronze >> transform >> sense_silver >> aggregate

github_pipeline_dag = github_pipeline_dag()