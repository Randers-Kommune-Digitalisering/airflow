import logging
from airflow.providers.http.hooks.http import HttpHook
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowFailException
from rkdigi.email_handling import EmailReader
from dag_vognpark.vognpark_data import (
    create_insubiz_vehicles_by_payloads,
    read_vehicle_ids_to_update_from_excel_bytes,
    close_insubiz_vehicles_by_ids,
    find_latest_attachment,
    read_vehicles_to_add_from_excel_bytes,
)

logger = logging.getLogger(__name__)


def process_vognpark_sync_changes() -> None:
    """
    Create and update vehicles in Insubiz based on the latest Excel data.
    """
    insubiz_hook = HttpHook(http_conn_id="insubiz_cloud_api")

    vognpark_imap_conn = BaseHook.get_connection("vognpark_imap")
    email_reader = EmailReader(
        email=vognpark_imap_conn.login,
        password=vognpark_imap_conn.password,
    )

    # Find the latest Vognpark Excel attachment in the email inbox(UNSEEN)
    found = find_latest_attachment(
        email_reader=email_reader,
        extensions=(".xlsx",),
        filename_prefixes=("uoverensstemmelser",),
    )

    if not found:
        raise AirflowFailException("No Vognpark Excel attachment found")

    uid, attachment_name, excel_bytes = found
    logger.info(
        f"Found Vognpark Excel in email UID {uid.decode()}: "
        f"{attachment_name} ({len(excel_bytes)} bytes)"
    )

    vehicle_ids_to_update = read_vehicle_ids_to_update_from_excel_bytes(
        excel_bytes=excel_bytes,
    )

    vehicles_to_create_payloads = read_vehicles_to_add_from_excel_bytes(
        excel_bytes=excel_bytes
    )

    updated_count = close_insubiz_vehicles_by_ids(
        http_hook=insubiz_hook,
        vehicle_ids=vehicle_ids_to_update,
    )

    created_count = create_insubiz_vehicles_by_payloads(
        http_hook=insubiz_hook,
        payloads=vehicles_to_create_payloads,
    )

    logger.info(f"Vognpark sync completed. Updated={updated_count}, Created={created_count}")
