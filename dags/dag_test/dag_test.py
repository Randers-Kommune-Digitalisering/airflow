import time
import logging

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from utils.config import DEFAULT_DAG_ARGS


dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["email_on_failure"] = False
dag_args["retries"] = 0

logger = logging.getLogger(__name__)


def test() -> None:
  logger.warning("Start")
  for i in range(1, 11):
    logging.info(f"{i}")
    time.sleep(3)
  logger.error("End")


with DAG(
    dag_id="test",
    start_date=datetime(year=2026, month=6, day=18, tz=timezone("Europe/Copenhagen")),
    schedule=None,
    render_template_as_native_obj=True,
    default_args=dag_args,
    catchup=False,
    max_active_runs=1,
    description="Test DAG - just writes logs",
    tags=['test']
) as dag:
    test = PythonOperator(
        task_id="test",
        python_callable=test
    )
