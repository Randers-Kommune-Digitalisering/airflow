from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime, timezone
from utils.config import DEFAULT_DAG_ARGS
from dag_affald.process_affald import process_affald

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 1


with DAG(
    dag_id="dag_affald",
    start_date=datetime(year=2026, month=1, day=30, tz=timezone("Europe/Copenhagen")),
    schedule="@monthly",
    catchup=False,
    default_args=dag_args,
    description="Fetch Affald data from Scanvaegt DB and store results in Excel, then email to recipients",
    tags=["affald", "scanvaegt_db", "excel", "email"],
) as dag:

    run_affald = PythonOperator(
        task_id="process_affald_task",
        python_callable=process_affald
    )
