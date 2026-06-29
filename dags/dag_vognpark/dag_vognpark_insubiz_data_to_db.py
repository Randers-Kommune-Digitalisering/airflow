from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime, timezone

from utils.config import DEFAULT_DAG_ARGS
from dag_vognpark.process_vognpark_insubiz_data_to_db import process_vognpark_insubiz_data_to_db

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 1


with DAG(
    dag_id="dag_vognpark_insubiz_data_to_db",
    start_date=datetime(year=2026, month=6, day=25, tz=timezone("Europe/Copenhagen")),
    schedule="@monthly",
    catchup=False,
    max_active_runs=1,
    default_args=dag_args,
    description="Fetch latest Vognpark data from Insubiz API and load it into Postgres",
    tags=["Flow 3", "Insubiz API", "Vognpark", "Postgres"],
) as dag:

    run_vognpark_insubiz_data_to_db = PythonOperator(
        task_id="process_vognpark_insubiz_data_to_db_task",
        python_callable=process_vognpark_insubiz_data_to_db,
    )
