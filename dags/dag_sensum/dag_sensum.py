from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime, timezone
from utils.config import DEFAULT_DAG_ARGS
from dag_sensum.process_sensum import process_sensum

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 1

with DAG(
    dag_id="dag_sensum",
    start_date=datetime(year=2026, month=1, day=6, tz=timezone("Europe/Copenhagen")),
    schedule="0 0 * * 1",
    catchup=False,
    default_args=dag_args,
    description="Fetch Sensum data from SFTP and load into Postgres",
    tags=["sensum", "sftp", "postgres"],
) as dag:

    run_sensum = PythonOperator(
        task_id="process_sensum_task", python_callable=process_sensum
    )
