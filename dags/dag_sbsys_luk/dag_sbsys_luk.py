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

# Filters for querying cases to close (set via Airflow Variables)
SBSYS_LUK_SAGSSKABELON_IDS = Variable.get("SBSYS_LUK_SAGSSKABELON_IDS", "")
SBSYS_LUK_SAGSSKABELON_IGNORE_IDS = Variable.get("SBSYS_LUK_SAGSSKABELON_IGNORE_IDS", "")
REQUIRED_SAGSSKABELON_IDS = [int(id.strip()) for id in SBSYS_LUK_SAGSSKABELON_IDS.split(",") if id.strip().isdigit()]
IGNORE_SAGSSKABELON_IDS = [int(id.strip()) for id in SBSYS_LUK_SAGSSKABELON_IGNORE_IDS.split(",") if id.strip().isdigit()]
REQUIRED_SAGSSTATUS = [status.strip() for status in Variable.get("SBSYS_LUK_SAGSSTATUS", "Aktiv").split(",") if status.strip()]  # Default to "Aktiv" if not set

with DAG(
    dag_id="dag_sbsys_luk",
    start_date=datetime(year=2026, month=3, day=9, tz=timezone("Europe/Copenhagen")),
    schedule="@weekly",
    catchup=False,
    default_args=dag_args,
    description="Fetch and close SBSYS cases based on specific criteria using SQL",
    tags=["sbsys", "sql"],
) as dag:

    run_sbsys_luk = PythonOperator(
        task_id="process_sbsys_luk_task",
        python_callable=process_sbsys_luk,
        op_kwargs={
            "required_sagsstatus": REQUIRED_SAGSSTATUS,
            "required_sagsskabelon_ids": REQUIRED_SAGSSKABELON_IDS,
            "ignore_sagsskabelon_ids": IGNORE_SAGSSKABELON_IDS,
            "dry_run": DRY_RUN
        },
    )
