import logging

# from airflow.exceptions import AirflowFailException

from dag_frontdesk.frontdesk_data import placeholder_function
from rkdigi.database_manager import DatabaseManager

logger = logging.getLogger(__name__)


def process_frontdesk() -> None:
    """
    Placeholder function for processing the frontdesk data.
    """
    logger.info("Starting to process frontdesk data...")
    placeholder_function()

    # TODO: Create this Airflow Connection with the same ID and correct connection type and details
    db_manager = DatabaseManager(
        profile_name="azure_frontdesk_db",
        db_type="mssql",
        airflow_connection_id="azure_frontdesk_db"
    )
    db_manager.can_connect()
    logger.info("DatabaseManager initialized for profile: frontdesk_db and connection: frontdesk_db")
