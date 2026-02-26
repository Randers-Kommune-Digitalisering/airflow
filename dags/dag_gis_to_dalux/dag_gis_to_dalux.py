from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime, timezone
from utils.config import DEFAULT_DAG_ARGS

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 1


def task_process_gis_to_dalux():
    from dag_gis_to_dalux.process_gis_to_dalux import process_gis_to_dalux
    return process_gis_to_dalux()


with DAG(
    dag_id="dag_gis_to_dalux",
    start_date=datetime(2025, 12, 17, tz=timezone("Europe/Copenhagen")),
    schedule="0 0 * * *",
    catchup=False,
    default_args=dag_args,
    description="Sync GIS building data into Dalux FM",
    tags=["dalux", "gis", "api", "postgres"],
) as dag:

    run_prod = PythonOperator(
        task_id="process_gis_to_dalux_task_full",
        python_callable=task_process_gis_to_dalux,
    )
