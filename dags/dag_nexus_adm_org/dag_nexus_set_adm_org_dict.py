import json
import logging
from pendulum import datetime, timezone
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

from utils.config import DEFAULT_DAG_ARGS
from dag_nexus_adm_org.delta import DeltaClient

logger = logging.getLogger(__name__)

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 0


def set_adm_org_var():
    top_uuid = Variable.get("nexus_top_adm_org_uuid", default_var=None)
    if not top_uuid:
        raise ValueError("Variable 'nexus_top_adm_org_uuid' is not set or is empty")
    delta_hook = BaseHook.get_connection('delta_prod')
    delta_client = DeltaClient(delta_hook=delta_hook, top_uuid=top_uuid)
    adm_org_list = delta_client.get_adm_org_list()
    Variable.set("nexus_adm_org_dict", json.dumps(adm_org_list))
    logger.info("Set 'nexus_adm_org_dict' variable with administrative organization units")


with DAG(
    dag_id="dag_nexus_set_adm_org_dict",
    start_date=datetime(year=2026, month=3, day=20, tz=timezone("Europe/Copenhagen")),
    schedule="@daily",
    default_args=dag_args,
    catchup=False,
    max_active_runs=1,
    description=(
        "Set the dict of administrative organization units for Nexus by querying Delta for the adm. org. units and checking which ones have employees. "
        "The dict contains uuids of adm. org. units that have employees as keys and lists of their sub adm. org. units' uuids as values."
    ),
    tags=['nexus', 'delta', 'permission', 'sync']
) as dag:

    task = PythonOperator(
        task_id="set_adm_org_dict_var",
        python_callable=set_adm_org_var
    )
