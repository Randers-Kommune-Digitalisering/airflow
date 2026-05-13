from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime, timezone

from utils.config import DEFAULT_DAG_ARGS
from dag_modregning.process_modregning import process_modregning

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 1

with DAG(
    dag_id="dag_modregning",
    start_date=datetime(year=2026, month=5, day=5, tz=timezone("Europe/Copenhagen")),
    schedule="0 9 15 * *",
    catchup=False,
    max_active_runs=1,
    default_args=dag_args,
    description="Fetch CPR list from SFTP, query Serviceplatform, and email Modregning report",
    tags=["modregning", "sftp", "serviceplatform", "email"],
) as dag:

    run_modregning = PythonOperator(
        task_id="process_modregning_task",
        python_callable=process_modregning,
    )
