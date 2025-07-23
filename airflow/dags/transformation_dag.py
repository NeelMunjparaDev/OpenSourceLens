from airflow import DAG
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import pytz
from gh_archive_pipeline.transformation.bronze_to_silver import main
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

@dag(
    dag_id='bronze_to_silver',
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
    description='Transform data from bronze to silver',
)
def bronze_to_silver_dag():
    @task(execution_timeout=timedelta(minutes=20))
    def transform_bronze_to_silver():
        print("Starting transform_bronze_to_silver")
        main()
        return "Transformed data to silver layer"

    trigger_silver_to_gold = TriggerDagRunOperator(
        task_id='trigger_aggregation',
        trigger_dag_id='silver_to_gold',
        conf={'execution_date': '{{ ds }}'},
        wait_for_completion=True,
        execution_timeout=timedelta(minutes=30),
        poke_interval=30,
    )

    transform = transform_bronze_to_silver()
    transform >> trigger_silver_to_gold

bronze_to_silver_instance = bronze_to_silver_dag()