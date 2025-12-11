
import logging

from dag_jobindsats.jobindsats_data import fetch_and_store_table_updates, get_data
from dag_jobindsats.jobindsats_config import JOBINDSATS_CONFIG
from utils.config import JOBINDSATS_HTTP_CONN_ID, JOBINDSATS_DB_CONN_ID

logger = logging.getLogger(__name__)


def process_jobindsats() -> None:
    try:
        fetch_and_store_table_updates(http_conn_id=JOBINDSATS_HTTP_CONN_ID, db_conn_id=JOBINDSATS_DB_CONN_ID)

        for job in JOBINDSATS_CONFIG:
            get_data(
                name=job['name'],
                years_back=job['years_back'],
                dataset=job['dataset'],
                period_format=job['period_format'],
                data_to_get=job['data_to_get'],
                http_conn_id=JOBINDSATS_HTTP_CONN_ID,
                db_conn_id=JOBINDSATS_DB_CONN_ID
            )

    except Exception as e:
        logger.exception(f"An error occurred: {e}")
        raise
