from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime, timezone

from utils.config import DEFAULT_DAG_ARGS
from dag_vognpark_compare_and_report.process_vognpark_compare_and_report import process_vognpark_compare_and_report

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 1


with DAG(
    dag_id="dag_vognpark_compare_and_report",
    start_date=datetime(year=2026, month=6, day=25, tz=timezone("Europe/Copenhagen")),
    schedule="@monthly",
    catchup=False,
    max_active_runs=1,
    default_args=dag_args,
    description="Compare Motorstyrelsen PDF with Insubiz API data, generate a report, and send it via email",
    tags=["Motorstyrelsen PDF", "Insubiz API", "Email report"],
) as dag:

    run_vognpark_compare_and_report = PythonOperator(
        task_id="process_vognpark_compare_and_report_task",
        python_callable=process_vognpark_compare_and_report,
    )
