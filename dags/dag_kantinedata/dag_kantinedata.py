from datetime import timedelta
from pendulum import datetime, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook

from utils.config import DEFAULT_DAG_ARGS
from dag_kantinedata.process_kantinedata import process_kantinedata

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 1
dag_args["retry_delay"] = timedelta(hours=12)


def get_config_start_main_flow() -> None:
    sftp_hook = SFTPHook(ssh_conn_id="kantinedata_sftp")
    process_kantinedata(sftp_hook=sftp_hook)


with DAG(
    dag_id="kantinedata",
    start_date=datetime(year=2026, month=4, day=8, tz=timezone("Europe/Copenhagen")),
    schedule='@daily',
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args=dag_args,
    description="Extract Kantinedata from email attachments and load into SFTP",
    tags=["kantinedata", "email", "sftp"],
) as dag:
    run_kantinedata = PythonOperator(
        task_id="process_kantinedata",
        python_callable=get_config_start_main_flow,
        max_active_tis_per_dag=1,
    )
