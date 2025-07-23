from airflow import DAG
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import pytz
from gh_archive_pipeline.transformation.silver_to_gold import main

@dag(
    dag_id='silver_to_gold',
    start_date=datetime(2024, 1, 1, tzinfo=pytz.timezone('Asia/Kolkata')),
    schedule=None,
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    catchup=False,
    max_active_runs=1,
    description='Perform aggregations',
)
def silver_to_gold_dag():
    @task(execution_timeout=timedelta(minutes=20))
    def perform_aggregations():
        print("Starting perform_aggregations")
        main()
        return "Aggregated data to gold layer"

    perform_aggregations()

silver_to_gold_instance = silver_to_gold_dag()