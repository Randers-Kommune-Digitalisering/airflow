import logging

from airflow.models import Variable
from airflow.hooks.base import BaseHook
# from airflow.exceptions import AirflowFailException

from dag_social_frikort.social_frikort_data import placeholder_function
from rkdigi.email_handling import EmailReader, EmailSender

logger = logging.getLogger(__name__)


def process_social_frikort() -> None:
    """
    Placeholder function for processing the social_frikort data.
    """
    logger.info("Starting to process social_frikort data...")
    placeholder_function()

    # TODO: Create this Airflow Connection with the same ID and correct connection type and details
    imap_conn = BaseHook.get_connection("social_frikort_imap")

    email_reader = EmailReader(
        email=imap_conn.login,
        password=imap_conn.password,
    )
    mailboxes = email_reader.list_mailboxes()
    logger.info(f"EmailReader initialized for connection: social_frikort_imap. Available mailboxes: {mailboxes}")

    # TODO: Create this Airflow Variable with the same name and correct JSON structure containing sender_email, recipient_emails, and smtp_server
    social_frikort_runtime_config = Variable.get("social_frikort_runtime_config", deserialize_json=True)

    sender = social_frikort_runtime_config["sender_email"]
    recipients = social_frikort_runtime_config["recipient_emails"]
    smtp_server = social_frikort_runtime_config["smtp_server"]

    email_sender = EmailSender(smtp_server=smtp_server)
    email_sender.send_email(
        sender=sender,
        recipients=recipients,
        subject="Placeholder subject",
        body="Placeholder body",
        attachments=[],
    )
