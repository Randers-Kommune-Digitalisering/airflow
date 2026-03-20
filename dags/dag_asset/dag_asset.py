from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime, timezone

from utils.config import DEFAULT_DAG_ARGS
from dag_asset.process_asset import (
    task_create_asset_tables,
    task_insert_departments_data,
    task_insert_department_ean_from_delta,
    task_insert_users_data,
    task_insert_computers_data,
    task_insert_atea_data,
    task_insert_device_license_and_historical_data,
    task_insert_ivanti_data,
    task_upload_pc_assets_to_topdesk,
    task_upload_mobile_assets_to_topdesk,
)
dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 1

with DAG(
    dag_id="dag_asset",
    start_date=datetime(year=2026, month=1, day=22, tz=timezone("Europe/Copenhagen")),
    schedule="@daily",
    catchup=False,
    default_args=dag_args,
    description="Fetch Asset data from CAPA CMS and load into Asset Postgres",
    tags=["capa_cms", "asset", "postgres", "topdesk", "atea", "ivanti"],
) as dag:
    t_create_tables = PythonOperator(
        task_id="create_asset_tables",
        python_callable=task_create_asset_tables,
    )

    t_departments = PythonOperator(
        task_id="insert_departments",
        python_callable=task_insert_departments_data,
    )

    t_delta_ean = PythonOperator(
        task_id="insert_department_ean_from_delta",
        python_callable=task_insert_department_ean_from_delta,
    )

    t_users = PythonOperator(
        task_id="insert_users",
        python_callable=task_insert_users_data,
    )

    t_fetch_ivanti_devices = PythonOperator(
        task_id="fetch_ivanti_devices",
        python_callable=task_insert_ivanti_data,
    )

    t_computers = PythonOperator(
        task_id="insert_computers",
        python_callable=task_insert_computers_data,
    )

    t_atea = PythonOperator(
        task_id="insert_atea",
        python_callable=task_insert_atea_data,
    )

    t_device_license = PythonOperator(
        task_id="insert_device_license_and_historical",
        python_callable=task_insert_device_license_and_historical_data,
    )

    t_upload_pc_assets_to_topdesk = PythonOperator(
        task_id="upload_pc_assets_to_topdesk",
        python_callable=task_upload_pc_assets_to_topdesk,
    )

    t_upload_mobile_assets_to_topdesk = PythonOperator(
        task_id="upload_mobile_assets_to_topdesk",
        python_callable=task_upload_mobile_assets_to_topdesk,
    )

    t_create_tables >> [t_departments, t_fetch_ivanti_devices]
    t_departments >> [t_users, t_delta_ean]

    [t_users, t_delta_ean, t_fetch_ivanti_devices] >> t_upload_mobile_assets_to_topdesk

    t_users >> t_computers >> t_atea >> t_device_license
    [t_delta_ean, t_device_license] >> t_upload_pc_assets_to_topdesk
