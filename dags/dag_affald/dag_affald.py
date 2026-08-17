from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime, timezone
from utils.config import DEFAULT_DAG_ARGS
from dag_affald.process_affald import (
    process_mp_affald,
    process_scanvaegt_affald,
)

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 1


with DAG(
    dag_id="dag_affald",
    start_date=datetime(year=2026, month=3, day=9, tz=timezone("Europe/Copenhagen")),
    schedule="0 7 5 * *",
    catchup=False,
    default_args=dag_args,
    description="Fetch Affald data from Scanvaegt DB + MP API and store results in Excel, then email to recipients",
    tags=["affald", "scanvaegt_db", "excel", "email", "mp_api"],
) as dag:

    run_scanvaegt_affald = PythonOperator(
        task_id="process_scanvaegt_affald_task",
        python_callable=process_scanvaegt_affald,
        do_xcom_push=False,
    )

    run_mp_affald = PythonOperator(
        task_id="process_mp_affald_task",
        python_callable=process_mp_affald,
        do_xcom_push=False
    )
