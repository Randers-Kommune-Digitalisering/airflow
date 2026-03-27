from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import pendulum
from pendulum import datetime, timezone
import json
from airflow.models import Variable
from utils.config import DEFAULT_DAG_ARGS
from dag_nexus_user_sync.delta import DeltaClient
from rkdigi.token_session import ManagedOAuth2Session

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 0


def testing():
    # Airflow Variables to import and pass to DeltaClient
    var_ids_to_import = ["nexus_adm_org_dict", "nexus_job_functions_to_import", "nexus_position_types_to_import"]
    delta_client_params = {}
    for var_id in var_ids_to_import:
        var_str = Variable.get(var_id, default_var=None)
        if var_str:
            try:
                var = json.loads(var_str)
            except Exception as e:
                raise ValueError(f"Failed to deserialize '{var_id}' variable: {e}")
        else:
            raise ValueError(f"Variable '{var_id}' is not set or is empty")
        # Remove 'nexus_' prefix to get the parameter name for DeltaClient
        param_name = var_id.replace("nexus_", "")
        delta_client_params[param_name] = var
    delta_client_params["changes_date"] = pendulum.DateTime.now(timezone("Europe/Copenhagen")).date()
    delta_hook = BaseHook.get_connection('delta_prod')
    delta_client = DeltaClient(delta_hook=delta_hook, **delta_client_params)
    user_changes = delta_client.get_employment_changes()
    print(user_changes)

    nexus_hook = BaseHook.get_hook("nexus_review")

    nexus_conn = nexus_hook.get_connection(nexus_hook.http_conn_id)
    nexus_session = ManagedOAuth2Session(
        token_url=nexus_conn.extra_dejson.get("token_url"),
        client_id=nexus_conn.login,
        client_secret=nexus_conn.password,
    )

    res = nexus_session.get(f"{nexus_conn.host.strip('/')}/api/core/mobile/randers/v2/")
    res.raise_for_status()
    home = res.json()

    res = nexus_session.get(home['_links']['suppliers']['href'])
    res.raise_for_status()
    all_suppliers = res.json()

    acvite_suppliers = [supplier for supplier in all_suppliers if supplier.get('active')]
    print(acvite_suppliers)


with DAG(
    dag_id="nexus_user_permission_sync",
    start_date=datetime(year=2026, month=3, day=20, tz=timezone("Europe/Copenhagen")),
    schedule="*/10 * * * *",
    default_args=dag_args,
    catchup=False,
    max_active_runs=1,
    description="Check Delta for employment changes and update users in Nexus accordingly",
    tags=['nexus', 'delta', 'permission', 'sync', 'user']
) as dag:

    task = PythonOperator(
        task_id="check_and_update_users_task",
        python_callable=testing
    )
