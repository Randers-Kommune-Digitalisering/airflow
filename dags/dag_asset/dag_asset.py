from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime, timezone
from utils.config import DEFAULT_DAG_ARGS
from dag_asset.process_asset import process_assets

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 1


with DAG(
    dag_id="dag_asset",
    start_date=datetime(year=2026, month=1, day=22, tz=timezone("Europe/Copenhagen")),
    schedule="@daily",
    catchup=False,
    default_args=dag_args,
    description="Fetch Asset data from CAPA CMS and load into Asset Postgres",
    tags=["capa_cms", "asset", "postgres", "topdesk"],
) as dag:

    run_asset = PythonOperator(
        task_id="process_asset_task",
        python_callable=process_assets
    )
