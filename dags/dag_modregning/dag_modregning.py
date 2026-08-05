from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from utils.config import DEFAULT_DAG_ARGS
from dag_modregning.process_modregning import process_modregning

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 0

with DAG(
    dag_id="dag_modregning",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=dag_args,
    description="Fetch CPR list from Mailbox, query Serviceplatform, and email Modregning report",
    tags=["modregning", "mailbox", "serviceplatform", "email"],
    params={
        "start_date": Param(
            default=None,
            type=["null", "string"],
            minLength=10,
            maxLength=10,
            description="Valgfri startdato i format YYYY-MM-DD fx 2026-06-01. Hvis tom, beregnes automatisk.",
        ),
        "end_date": Param(
            default=None,
            type=["null", "string"],
            minLength=10,
            maxLength=10,
            description="Valgfri slutdato i format YYYY-MM-DD fx 2026-07-27. Hvis tom, beregnes automatisk.",
        ),
    },
) as dag:

    run_modregning = PythonOperator(
        task_id="process_modregning_task",
        python_callable=process_modregning,
        do_xcom_push=False
    )
