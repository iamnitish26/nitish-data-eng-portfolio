from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {"owner": "nitish", "retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="batch_trips_etl",
    start_date=datetime(2025, 9, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["batch", "spark", "glue"],
) as dag:

    extract = BashOperator(
        task_id="extract",
        bash_command="python ../projects/01-batch-s3-glue-emr/src/etl_batch.py --input ../../data/raw/trips_sample.csv --output ../../data/curated",
    )

    dq = BashOperator(
        task_id="dq_check", bash_command="echo 'Run Great Expectations here (stub)'"
    )

    extract >> dq
