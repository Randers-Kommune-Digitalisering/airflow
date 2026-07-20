import logging
import json

from typing import Any
from dag_fritidsjobs_webscraper.scapy_client import scrape_sites
from dag_fritidsjobs_webscraper.utils.email_construction import construct_email
from dag_fritidsjobs_webscraper.db_client import filter_existing_jobs, insert_jobs
from dag_fritidsjobs_webscraper.utils.model import Base

from airflow.models import Variable
from airflow.exceptions import AirflowFailException
from rkdigi.email_handling import EmailSender
from rkdigi.database_manager import DatabaseManager


logger = logging.getLogger(__name__)


def process_fritidsjobs_webscraper() -> list[dict[str, Any]]:
    """
    Scrape each configured site and list.

    :return: JSON-serializable site results grouped by site and list
    """
    logger.info("Starting to process fritidsjobs_webscraper data...")
    db_session = None

    # Check runtime config
    runtime_config = Variable.get("fritidsjobs_webscraper_runtime_config", deserialize_json=True)

    site_configs = runtime_config.get("sites", [])
    if not isinstance(site_configs, list):
        raise TypeError("runtime_config['sites'] must be a list of dicts")

    if not all(isinstance(site_config, dict) for site_config in site_configs):
        raise TypeError("runtime_config['sites'] must contain site config objects")

    sender = runtime_config["sender_email"]
    recipients = runtime_config["recipient_emails"]
    smtp_server = runtime_config["smtp_server"]

    if not all(isinstance(email, str) for email in [sender] + recipients):
        raise TypeError("runtime_config['sender_email'] and runtime_config['recipient_emails'] must be strings")

    # Initialize DatabaseManager
    try:
        db_manager = DatabaseManager(
            profile_name="fritidsjobs_webscraper_db",
            db_type="postgres",
            airflow_connection_id="fritidsjobs_webscraper_db",
            base_model=Base,
        )
        db_session = db_manager.get_session()
        logger.info("DatabaseManager initialized for profile: fritidsjobs_webscraper_db and connection: fritidsjobs_webscraper_db")
    except Exception as e:
        logger.error("Failed to initialize DatabaseManager: %s", str(e))
        raise AirflowFailException(f"Failed to initialize DatabaseManager: {str(e)}") from e

    # Scrape sites
    try:
        scraped_sites = scrape_sites(site_configs)
        logger.info("Finished scraping fritidsjobs_webscraper data: %s", json.dumps(scraped_sites, ensure_ascii=False))
    except Exception as e:
        logger.error("Error during scraping: %s", str(e))
        raise AirflowFailException(f"Error during scraping: {str(e)}") from e

    # Filter out existing jobs from the database
    try:
        filtered_jobs = filter_existing_jobs(db_session, scraped_sites)
        logger.info("Filtered jobs: %s", json.dumps(filtered_jobs, ensure_ascii=False))
    except Exception as e:
        logger.error("Error filtering existing jobs: %s", str(e))
        raise AirflowFailException(f"Error filtering existing jobs: {str(e)}") from e

    subject, email_body = construct_email(filtered_jobs)
    logger.info("Constructed email subject: %s", subject)
    logger.info("Constructed email body: %s", email_body)

    # If no new jobs are found, log and exit without sending an email
    if not filtered_jobs:
        logger.info("No new jobs found. Exiting without sending email.")

        if db_session is not None:
            db_session.close()
        return []

    # Send email with new jobs
    email_sender = EmailSender(smtp_server=smtp_server)
    try:
        email_sender.send_email(
            sender=sender,
            recipients=recipients,
            subject=subject,
            body=email_body,
            attachments=[],
        )
    except Exception as e:
        logger.error("Failed to send email: %s", str(e))
        raise AirflowFailException(f"Failed to send email: {str(e)}") from e

    # Insert new jobs into the database
    try:
        insert_jobs(db_session, filtered_jobs)
        logger.info("Inserted new jobs into the database successfully.")
    except Exception as e:
        logger.error("Failed to insert jobs into the database: %s", str(e))
        raise AirflowFailException(f"Failed to insert jobs into the database: {str(e)}") from e

    if db_session is not None:
        db_session.close()
    return
