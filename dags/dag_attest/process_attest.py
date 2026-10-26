import logging

from airflow.models import Variable
from airflow.hooks.base import BaseHook
# from airflow.exceptions import AirflowFailException

from dag_attest.attest_data import placeholder_function
from rkdigi.email_handling import EmailReader, EmailSender
from rkdigi.database_manager import DatabaseManager

logger = logging.getLogger(__name__)


def process_attest() -> None:
    """
    Placeholder function for processing the attest data.
    """
    logger.info("Starting to process attest data...")
    placeholder_function()

    # TODO: Create this Airflow Connection with the same ID and correct connection type and details
    imap_conn = BaseHook.get_connection("attest_imap")

    email_reader = EmailReader(
        email=imap_conn.login,
        password=imap_conn.password,
    )
    mailboxes = email_reader.list_mailboxes()
    logger.info(f"EmailReader initialized for connection: attest_imap. Available mailboxes: {mailboxes}")

    # TODO: Create this Airflow Variable with the same name and correct JSON structure containing sender_email, recipient_emails, and smtp_server
    attest_runtime_config = Variable.get("attest_runtime_config", deserialize_json=True)

    sender = attest_runtime_config["sender_email"]
    recipients = attest_runtime_config["recipient_emails"]
    smtp_server = attest_runtime_config["smtp_server"]

    email_sender = EmailSender(smtp_server=smtp_server)
    email_sender.send_email(
        sender=sender,
        recipients=recipients,
        subject="Placeholder subject",
        body="Placeholder body",
        attachments=[],
    )

    # TODO: Create this Airflow Connection with the same ID and correct connection type and details
    db_manager = DatabaseManager(
        profile_name="attest_db",
        db_type="postgres",
        airflow_connection_id="attest_db"
    )
    db_manager.can_connect()
    logger.info("DatabaseManager initialized for profile: attest_db and connection: attest_db")
