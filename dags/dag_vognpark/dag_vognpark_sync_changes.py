from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime, timezone

from utils.config import DEFAULT_DAG_ARGS
from dag_vognpark.process_vognpark_sync_changes import process_vognpark_sync_changes

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 1


with DAG(
    dag_id="dag_vognpark_sync_changes",
    start_date=datetime(year=2026, month=6, day=25, tz=timezone("Europe/Copenhagen")),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=dag_args,
    description="Synchronize changes by creating and deleting vehicles in Insubiz based on the latest Vognpark Excel data",
    tags=["Flow 2", "Create Vehicles", "Delete Vehicles", "Insubiz API", "Vognpark Excel"],
) as dag:

    run_vognpark_sync_changes = PythonOperator(
        task_id="process_vognpark_sync_changes_task",
        python_callable=process_vognpark_sync_changes,
    )
