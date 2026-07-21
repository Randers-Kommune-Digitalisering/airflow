import logging

# from airflow.hooks.base import BaseHook
# from airflow.exceptions import AirflowFailException

# from rkdigi.email_handling import EmailReader
# from rkdigi.database_manager import DatabaseManager

logger = logging.getLogger(__name__)


def process_mailbox_cleaner() -> None:
    """
    Placeholder function for processing the mailbox_cleaner data.
    """
    logger.info("Starting to process mailbox_cleaner data...")

    demo_config = [{
        "id": "invoices_cleanup",
        "enabled": True,
        "mail_connection_id": "inbox_cleaner_demo_imap",
        "mailbox": "INBOX",
        "match_mode": "all",
        "requirements": {
            "subject": {
                "contains_any": [
                    "Invoice",
                    "Payment"
                ],
                "contains_all": [
                    "Invoice",
                    "Payment"
                ],
                "regex": [
                    ".*(Invoice|Payment).*"
                ]
            },
            "from": {
                "match": [
                    "example@example.com"
                ],
                "not_match": [
                    "noreply@example.com"
                ],
                "regex": [
                    ".*@example\\.com"
                ]
            },
            "flags": {
                "include_all": [
                    "\\Seen"
                ],
                "include_any": [
                    "\\Seen"
                ],
                "exclude_any": [
                    "\\Flagged"
                ],
                "exclude_all": [
                    "\\Flagged"
                ]
            },
            "age": {
                "older_than_days": 30,
                "older_than_hours": 720,
                "newer_than_days": 7,
                "newer_than_hours": 168
            },
            "attachments": {
                "has_attachments": True,
                "type": [
                    "pdf",
                    "docx"
                ],
                "name": {
                    "contains_any": [
                        "invoice",
                        "payment"
                    ]
                }
            }
        },
        "action": {
            "type": "move",  # Possible actions: "archive", "delete", "move_to_folder"
            "target_mailbox": "Archive/Finance"
        },
        "safety": {
            "dry_run": True,
            "max_messages_per_run": 200,
            "min_age_for_delete_days": 14
        }
    }]

    return demo_config

    # # TODO: Create this Airflow Connection with the same ID and correct connection type and details
    # imap_conn = BaseHook.get_connection("mailbox_cleaner_imap")

    # email_reader = EmailReader(
    #     email=imap_conn.login,
    #     password=imap_conn.password,
    # )
    # mailboxes = email_reader.list_mailboxes()
    # logger.info(f"EmailReader initialized for connection: mailbox_cleaner_imap. Available mailboxes: {mailboxes}")

    # # TODO: Create this Airflow Connection with the same ID and correct connection type and details
    # db_manager = DatabaseManager(
    #     profile_name="mailbox_cleaner_db",
    #     db_type="postgres",
    #     airflow_connection_id="mailbox_cleaner_db"
    # )
    # db_manager.can_connect()
    # logger.info("DatabaseManager initialized for profile: mailbox_cleaner_db and connection: mailbox_cleaner_db")
