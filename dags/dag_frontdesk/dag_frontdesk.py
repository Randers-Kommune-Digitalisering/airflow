from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime, timezone

from utils.config import DEFAULT_DAG_ARGS
from dag_frontdesk.process_frontdesk import process_frontdesk

dag_args = DEFAULT_DAG_ARGS.copy()
# TODO: tilføj retry til 1
dag_args["retries"] = 0


with DAG(
    # TODO: Bedre navn for DAG-id: f.eks. azure_frontdesk_data_to_postgres_db
    dag_id="dag_frontdesk",
    start_date=datetime(year=2026, month=9, day=15, tz=timezone("Europe/Copenhagen")),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    default_args=dag_args,
    description="Fetch data from frontdesk mssql db and upload to postgres",
    tags=["frontdesk", "mssql", "postgres"],
) as dag:

    run_frontdesk = PythonOperator(
        task_id="process_frontdesk_task",
        python_callable=process_frontdesk,
        # TODO: tilføj do_xcom_push parameter til tasken
    )
