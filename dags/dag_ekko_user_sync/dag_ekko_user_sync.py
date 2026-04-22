from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime, timezone
from airflow.models import Variable
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.ftp.hooks.ftp import FTPHook

from utils.config import DEFAULT_DAG_ARGS
from dag_ekko_user_sync.main_flow import get_ekko_sd_departments, get_ekko_sd_user_data, upload_ekko_users

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 0

ekko_sd_departments = Variable.get("ekko_sd_departments", default_var=None)
sd_http_hook = HttpHook(method="POST", http_conn_id="sd_silkeborgdata")
ekko_ftps_hook = FTPHook(ftp_conn_id="ekko_ftps")


with DAG(
    dag_id="ekko_user_sync",
    start_date=datetime(2025, 4, 21, tz=timezone("Europe/Copenhagen")),
    schedule="@daily",
    catchup=False,
    default_args=dag_args,
    description="Sync users from SD (Ejendomsservice) to Ekko App",
    tags=["ekko", "sd", "user", "sync"],
) as dag:

    sd_departments = PythonOperator(
        task_id="get_ekko_sd_departments",
        python_callable=get_ekko_sd_departments,
        op_kwargs={
            "ekko_sd_departments_str": ekko_sd_departments,
            "sd_http_hook": sd_http_hook
        }
    )

    ekko_users = PythonOperator(
        task_id="get_ekko_sd_user_data",
        python_callable=get_ekko_sd_user_data,
        op_kwargs={
            "sd_departments_task_id": "get_ekko_sd_departments",
            "sd_http_hook": sd_http_hook
        }
    )

    uploaded = PythonOperator(
        task_id="upload_ekko_users",
        python_callable=upload_ekko_users,
        op_kwargs={
            "sd_user_data_task_id": "get_ekko_sd_user_data",
            "ekko_ftps_hook": ekko_ftps_hook
        }
    )

    sd_departments >> ekko_users >> uploaded
