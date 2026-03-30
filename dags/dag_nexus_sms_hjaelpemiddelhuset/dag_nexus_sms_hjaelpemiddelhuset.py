from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime, timezone
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.providers.http.hooks.http import HttpHook

from utils.config import DEFAULT_DAG_ARGS
from dag_nexus_sms_hjaelpemiddelhuset.nexus import send_sms_for_hjaelpemiddelhuset_orders

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 1


def get_config_and_and_call_main_function() -> bool:
    """Fetches necessary configuration from Airflow Variables and Connections, then calls the main function to send SMS notifications for Hjaelpemiddelhuset orders."""
    door_codes = Variable.get("hjaelpemiddelhuset_door_codes", deserialize_json=True)
    nexus_hook = BaseHook.get_hook("nexus_prod")
    sms_hook = HttpHook(method='POST', http_conn_id='computronic_89158600')

    return send_sms_for_hjaelpemiddelhuset_orders(nexus_hook, sms_hook, door_codes)


with DAG(
    dag_id="nexus_sms_hjaelpemiddelhuset",
    start_date=datetime(year=2026, month=3, day=27, tz=timezone("Europe/Copenhagen")),
    schedule="*/5 * * * *",
    catchup=False,
    default_args=dag_args,
    description="Fetch Nexus data from Hjaelpemiddelhuset, send SMS notifications via Computronic and update Nexus orders accordingly",
    tags=["nexus", "computronic", "sms", "hjaelpemiddelhuset"],
) as dag:

    run_affald = PythonOperator(
        task_id="send_sms_for_hjaelpemiddelhuset_orders",
        python_callable=get_config_and_and_call_main_function
    )
