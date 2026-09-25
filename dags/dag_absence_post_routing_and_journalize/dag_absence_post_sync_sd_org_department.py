from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.config import DEFAULT_DAG_ARGS
from dag_absence_post_routing_and_journalize.process_absence_post_routing_and_journalize import (
    sync_sd_org_department_mapping
)

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 0


with DAG(
    dag_id="absence_post_sync_sd_org_department_mapping",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=dag_args,
    description="Sync SD org department mapping to Airflow Variable 'absence_post_mapning'",
    tags=["sync sd org department", "excel", "airflow variable"],
) as dag:

    sync_sd_org_department_mapping_task = PythonOperator(
        task_id="sync_sd_org_department_mapping_task",
        python_callable=sync_sd_org_department_mapping,
        do_xcom_push=False,
    )
