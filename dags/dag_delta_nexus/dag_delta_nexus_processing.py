import logging
from datetime import date, timedelta
from html import escape

import pendulum
from requests.exceptions import HTTPError
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email

from dag_delta_nexus.delta import DeltaClient
from dag_delta_nexus.nexus import NexusClient, DeltaPerson, ProfessionalStsSkipped
from utils.config import DEFAULT_DAG_ARGS


logger = logging.getLogger(__name__)
dag_args = {**DEFAULT_DAG_ARGS, "retries": 2, "retry_delay": timedelta(minutes=2)}


def failure_reason(error: Exception) -> str:
    if isinstance(error, ProfessionalStsSkipped):
        return "Brugeren kunne ikke hentes fra støttesystemet efter gentagne forsøg. Flere brugere?"
    reasons = {
        "Professional lookup returned multiple matches": "Flere Nexus-brugere matcher brugerens ADGUID.",
        "STS import or linking did not return a professional": "Brugeren kunne ikke importeres eller tilknyttes fra støttesystemet.",
        "Professional creation is disabled": "Brugeren findes ikke i Nexus, og oprettelse er slået fra.",
        "Professional configuration cannot be updated": "Nexus-brugerens konfiguration kunne ikke hentes.",
    }
    return reasons.get(str(error), f"Uventet fejl under behandlingen ({type(error).__name__}).")


def fetch_user_details(**context) -> None:
    """Process selected Nexus users and email a Danish change report."""
    conf = context["dag_run"].conf or {}
    user_uuids = conf.get("user_uuids")
    if not isinstance(user_uuids, list) or not user_uuids or not all(
        isinstance(uuid, str) for uuid in user_uuids
    ):
        raise ValueError("Expected a non-empty list of user UUIDs")
    valid_date = date.fromisoformat(conf["valid_date"])

    delta_conn = BaseHook.get_connection("delta_prod")
    delta_client = DeltaClient(
        base_url=delta_conn.host,
        token_url=delta_conn.extra_dejson["token_url"],
        client_id=delta_conn.login,
        client_secret=delta_conn.password,
    )

    user_details = delta_client.get_user_details(user_uuids, valid_date)
    nexus_persons = [DeltaPerson.from_dict(user) for user in user_details]
    show_failed_only = Variable.get("DELTA_NEXUS_SHOW_FAILED_ONLY", default_var="false").strip().lower() == "true"
    failed_users = []
    report_items = []

    nexus_conn = BaseHook.get_connection("nexus_prod")
    nexus_client = NexusClient(
        base_url=nexus_conn.host,
        token_url=nexus_conn.extra_dejson["token_url"],
        client_id=nexus_conn.login,
        client_secret=nexus_conn.password,
    )

    for person in nexus_persons:
        failed = False
        try:
            changes = nexus_client.update_existing_professional(person=person)
        except Exception as error:
            failed = True
            failed_users.append(person.user.ad_guid)
            if isinstance(error, HTTPError) and error.response is not None:
                logger.error(f"Professional update failed (HTTP {error.response.status_code}, content={error.response.text!r})")
            else:
                logger.error("Professional update failed (%s)", type(error).__name__)
            changes = [f"Behandling mislykkedes: {failure_reason(error)}"]
        if not changes:
            changes = ["Ingen ændringer."]
        sublist_style = ' style="color: #000; font-weight: normal;"' if failed else ""
        details = f"<ul{sublist_style}>" + "".join(f"<li>{escape(change)}</li>" for change in changes) + "</ul>"
        item_style = ' style="color: #b91c1c; font-weight: bold;"' if failed else ""
        if not show_failed_only or failed:
            report_items.append(f"<li{item_style}>{escape(person.name)} ({escape(person.user.username)}){details}</li>")

    if failed_users:
        raise RuntimeError(f"Failed to update {len(failed_users)} Nexus professionals")

    if report_items:
        sent_at = pendulum.now("Europe/Copenhagen").strftime("%H:%M %d-%m-%Y")
        send_email(
            to=["rune.aagaard.keena@randers.dk"],  # , "Jane.Scharling.Andersen@randers.dk", "Natashia.Maise.Nygaard@randers.dk"],
            subject=f"Delta Nexus - ændringsrapport - {sent_at}",
            html_content="<ul>" + "".join(report_items) + "</ul>",
        )


with DAG(
    dag_id="delta_nexus_processing",
    start_date=pendulum.datetime(2026, 11, 3, tz="UTC"),
    schedule=None,
    default_args=dag_args,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    description="Process Nexus users affected by Delta changes",
    tags=["delta", "nexus", "processing", "user"],
) as dag:
    PythonOperator(
        task_id="fetch_user_details",
        python_callable=fetch_user_details,
        do_xcom_push=False,
        show_return_value_in_logs=False,
    )
