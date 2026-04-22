from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime, timezone
from utils.config import DEFAULT_DAG_ARGS
from dags.dag_sbsys_luk import process_sbsys_luk

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 1


with DAG(
    dag_id="dag_sbsys_luk",
    start_date=datetime(year=2026, month=3, day=9, tz=timezone("Europe/Copenhagen")),
    schedule="@monthly",
    catchup=False,
    default_args=dag_args,
    description="Fetch and close SBSYS cases based on specific criteria using SQL",
    tags=["sbsys", "sql"],
) as dag:

    run_sbsys_luk = PythonOperator(
        task_id="process_sbsys_luk_task",
        python_callable=process_sbsys_luk
    )
