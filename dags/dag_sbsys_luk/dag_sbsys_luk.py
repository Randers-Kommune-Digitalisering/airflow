from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from pendulum import datetime, timezone
from utils.config import DEFAULT_DAG_ARGS
from dag_sbsys_luk.process_sbsys_luk import process_sbsys_luk

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 0

# DRY_RUN: set to True to log intended updates without making changes, False to perform updates
DRY_RUN = Variable.get("SBSYS_LUK_DRY_RUN", default_var="True").lower() == "true"
SBSYS_LUK_SAGSSKABELON_IDS = Variable.get("SBSYS_LUK_SAGSSKABELON_IDS", default_var="5133")
SAGSSKABELON_IDS = [int(id.strip()) for id in SBSYS_LUK_SAGSSKABELON_IDS.split(",") if id.strip().isdigit()]


with DAG(
    dag_id="dag_sbsys_luk",
    start_date=datetime(year=2026, month=3, day=9, tz=timezone("Europe/Copenhagen")),
    schedule="@monthly",
    catchup=False,
    default_args=dag_args,
    description="Fetch and close SBSYS cases based on specific criteria using SQL",
    tags=["sbsys", "sql"],
) as dag:

    run_sbsys_luk = PythonOperator(
        task_id="process_sbsys_luk_task",
        python_callable=process_sbsys_luk,
        op_kwargs={
            "sagsskabelon_ids": SAGSSKABELON_IDS,
            "dry_run": DRY_RUN
        },
    )
