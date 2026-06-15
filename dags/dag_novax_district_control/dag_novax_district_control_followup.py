from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from pendulum import datetime, timezone

from utils.config import DEFAULT_DAG_ARGS
from dag_novax_district_control.check_and_update_district_followup import check_and_update_district_followup

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 0

# DRY_RUN: set to True to log intended updates without making changes, False to perform updates
DRY_RUN = Variable.get("NOVAX_DRY_RUN", default_var="True").lower() == "true"
IGNORE_CPRS = Variable.get("NOVAX_IGNORE_CPRS", default_var="").split(",")

with DAG(
    dag_id="dag_novax_district_control_followup",
    start_date=datetime(year=2026, month=6, day=15, tz=timezone("Europe/Copenhagen")),
    schedule="15 1 * * 1",
    default_args=dag_args,
    catchup=False,
    max_active_runs=1,
    description="Check and update Novax district records by querying relevant clients",
    tags=['novax', 'district', 'dataforsyning', 'cpr'],
) as dag:

    task = PythonOperator(
        task_id="check_and_update_district_followup_task",
        python_callable=check_and_update_district_followup,
        op_kwargs={
            "dry_run": DRY_RUN,
            "ignore_cprs": IGNORE_CPRS
        },
    )
