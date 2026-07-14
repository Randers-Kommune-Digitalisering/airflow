import logging
from typing import Any
from dag_fritidsjobs_webscraper.scapy_client import scrape_sites
from dag_fritidsjobs_webscraper.email_construction import construct_email

from airflow.models import Variable
# from airflow.hooks.base import BaseHook
# from airflow.exceptions import AirflowFailException
# from dag_fritidsjobs_webscraper.fritidsjobs_webscraper_data import placeholder_function
from rkdigi.email_handling import EmailSender
# from rkdigi.database_manager import DatabaseManager

import json

logger = logging.getLogger(__name__)


def process_fritidsjobs_webscraper() -> list[dict[str, Any]]:
    """
    Scrape each configured site and list.

    :return: JSON-serializable site results grouped by site and list
    """
    logger.info("Starting to process fritidsjobs_webscraper data...")

    runtime_config = Variable.get("fritidsjobs_webscraper_runtime_config", deserialize_json=True)

    sites_config = runtime_config.get("sites", [])
    if not isinstance(sites_config, list):
        raise TypeError("runtime_config['sites'] must be a list of dicts")

    sender = runtime_config["sender_email"]
    recipients = runtime_config["recipient_emails"]
    smtp_server = runtime_config["smtp_server"]

    if not all(isinstance(email, str) for email in [sender] + recipients):
        raise TypeError("runtime_config['sender_email'] and runtime_config['recipient_emails'] must be strings")

    site_configs = sites_config

    if not all(isinstance(site_config, dict) for site_config in site_configs):
        raise TypeError("runtime_config['sites'] must contain site config objects")

    scraped_sites = scrape_sites(site_configs)
    logger.info("Finished scraping fritidsjobs_webscraper data: %s", json.dumps(scraped_sites, ensure_ascii=False))

    subject, email_body = construct_email(scraped_sites)
    logger.info("Constructed email subject: %s", subject)
    logger.info("Constructed email body: %s", email_body)

    email_sender = EmailSender(smtp_server=smtp_server)
    email_sender.send_email(
        sender=sender,
        recipients=recipients,
        subject=subject,
        body=email_body,
        attachments=[],
    )

    # return scraped_sites

    # placeholder_function()

    # sender = runtime_config["sender_email"]
    # recipients = runtime_config["recipient_emails"]
    # smtp_server = runtime_config["smtp_server"]

    # email_sender = EmailSender(smtp_server=smtp_server)
    # email_sender.send_email(
    #     sender=sender,
    #     recipients=recipients,
    #     subject="Placeholder subject",
    #     body="Placeholder body",
    #     attachments=[],
    # )

    # db_manager = DatabaseManager(
    #     profile_name="fritidsjobs_webscraper_db",
    #     db_type="postgres",
    #     airflow_connection_id="fritidsjobs_webscraper_db"  # remember to create this Airflow Connection with the same ID and correct connection type and details
    # )
    # db_manager.can_connect()
    # logger.info("DatabaseManager initialized for profile: fritidsjobs_webscraper_db and connection: fritidsjobs_webscraper_db")
