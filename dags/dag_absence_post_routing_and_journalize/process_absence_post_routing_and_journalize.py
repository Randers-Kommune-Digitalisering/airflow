import logging
import json

from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowFailException

from dag_absence_post_routing_and_journalize.absence_post_routing_and_journalize_data import (
    build_department_email_map
)
from rkdigi.email_handling import EmailReader
from utils.mail_attachments import find_latest_attachment

logger = logging.getLogger(__name__)


def sync_sd_org_department_mapping() -> None:
    """
    Placeholder function for processing the absence_post_routing_and_journalize data.
    """
    logger.info("Starting to process absence_post_routing_and_journalize data...")
    absence_post_imap_conn = BaseHook.get_connection("absence_post_imap")
    absence_post_config = Variable.get("absence_post_config", deserialize_json=True)

    email_reader = EmailReader(
        email=absence_post_imap_conn.login,
        password=absence_post_imap_conn.password,
        imap_server=absence_post_config.get("imap_server")
    )

    # Find the newest matching Excel attachment in the mailbox
    found = find_latest_attachment(
        email_reader=email_reader,
        filename_prefixes="SD org"
    )

    if not found:
        raise AirflowFailException("No matching attachment found in the mailbox.")

    uid, filename, content_bytes = found
    logger.info(f"Found Excel attachment in email UID {uid.decode()}: {filename} ({len(content_bytes)} bytes)")

    # Store the map for the Airflow Variable
    department_email_map = build_department_email_map(content_bytes)
    Variable.set("absence_post_mapning", json.dumps(department_email_map, ensure_ascii=False, indent=2))
    logger.info(f"Updated Airflow Variable 'absence_post_mapning' with {len(department_email_map)} department(s).")
