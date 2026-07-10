import logging
from typing import Any
from dag_fritidsjobs_webscraper.scapy_client import scrape_sites

# from airflow.models import Variable
# from airflow.hooks.base import BaseHook
# from airflow.exceptions import AirflowFailException
# from dag_fritidsjobs_webscraper.fritidsjobs_webscraper_data import placeholder_function
# from rkdigi.email_handling import EmailSender
# from rkdigi.database_manager import DatabaseManager

import json

logger = logging.getLogger(__name__)


def process_fritidsjobs_webscraper() -> list[dict[str, Any]]:
    """
    Scrape each configured site and list.

    :return: JSON-serializable site results grouped by site and list
    """
    logger.info("Starting to process fritidsjobs_webscraper data...")

    demo_config = [
        {
            "site_name": "Jem & Fix",
            "site_url": "https://www.jemogfix.dk/job-karriere/ledige-stillinger/",
            "allowed_domains": [
                "api.teamtailor.com",
                "scripts.teamtailor-cdn.com",
            ],
            "lists": [
                {
                    "list_name": "Randers N",
                    "list_route": [
                        {
                            "wait_for": "div.teamtailor-jobs__filters > div:nth-child(3) > select.teamtailor-jobs__select"
                        },
                        {
                            "select": {
                                "selector": "div.teamtailor-jobs__filters > div:nth-child(3) > select.teamtailor-jobs__select",
                                "label": "Randers N",
                            }
                        },
                    ],
                    "list_elements": {
                        "title": "div.teamtailor-jobs__job-wrapper a.teamtailor-jobs__job-title",
                        "link": "div.teamtailor-jobs__job-wrapper a.teamtailor-jobs__job-title",
                    }
                }
            ]
        }
    ]

    scraped_sites = scrape_sites(demo_config)
    logger.info("Finished scraping fritidsjobs_webscraper data: %s", json.dumps(scraped_sites, ensure_ascii=False))
    return scraped_sites

    # placeholder_function()

    # fritidsjobs_webscraper_runtime_config = Variable.get("fritidsjobs_webscraper_runtime_config", deserialize_json=True) # remember to create this Airflow Variable with the same name and correct JSON structure containing sender_email, recipient_emails, and smtp_server

    # sender = fritidsjobs_webscraper_runtime_config["sender_email"]
    # recipients = fritidsjobs_webscraper_runtime_config["recipient_emails"]
    # smtp_server = fritidsjobs_webscraper_runtime_config["smtp_server"]

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
