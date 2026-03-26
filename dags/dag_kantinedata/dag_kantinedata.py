from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime, timezone

from utils.config import DEFAULT_DAG_ARGS
from dag_kantinedata.process_kantinedata import process_kantinedata

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 0

with DAG(
    dag_id="dag_kantinedata",
    start_date=datetime(year=2026, month=1, day=16, tz=timezone("Europe/Copenhagen")),
    schedule='@daily',
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args=dag_args,
    description="Extract Kantinedata from email attachments and load into SFTP",
    tags=["kantinedata", "email", "sftp"],
) as dag:
    run_kantinedata = PythonOperator(
        task_id="process_kantinedata_task",
        python_callable=process_kantinedata,
        max_active_tis_per_dag=1,
    )
