from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.config import DEFAULT_DAG_ARGS
from dag_sd_fleksjobrefusion.process_sd_fleksjobrefusion import (
    process_sd_fleksjobrefusion,
)

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 0


with DAG(
    dag_id="fleksjobrefusion_registration_sd_personaleweb",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=dag_args,
    description="Login and run SD Fleksjobrefusion browser flow",
    tags=["sd_fleksjobrefusion", "sd", "rpa"],
) as dag:

    run_sd_fleksjobrefusion = PythonOperator(
        task_id="process_sd_fleksjobrefusion_task",
        python_callable=process_sd_fleksjobrefusion,
        do_xcom_push=False
    )
