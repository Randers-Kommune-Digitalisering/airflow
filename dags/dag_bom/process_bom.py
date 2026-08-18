import logging

# from airflow.exceptions import AirflowFailException

from dag_bom.bom_data import placeholder_function
from rkdigi.database_manager import DatabaseManager

logger = logging.getLogger(__name__)


def process_bom() -> None:
    """
    Placeholder function for processing the bom data.
    """
    logger.info("Starting to process bom data...")
    placeholder_function()

    # TODO: Create this Airflow Connection with the same ID and correct connection type and details
    db_manager = DatabaseManager(
        profile_name="bom_db",
        db_type="postgres",
        airflow_connection_id="bom_db"
    )
    db_manager.can_connect()
    logger.info("DatabaseManager initialized for profile: bom_db and connection: bom_db")
