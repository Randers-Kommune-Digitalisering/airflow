
import logging

from dag_jobindsats.jobindsats_data import fetch_and_store_table_updates, get_data
from dag_jobindsats.jobindsats_config import JOBINDSATS_CONFIG

logger = logging.getLogger(__name__)


def process_jobindsats(http_conn_id: str = "jobindsats_api", db_conn_id: str = "jobindsats_db") -> None:
    fetch_and_store_table_updates(http_conn_id=http_conn_id, db_conn_id=db_conn_id)
    for job in JOBINDSATS_CONFIG:
        get_data(
            name=job['name'],
            years_back=job['years_back'],
            dataset=job['dataset'],
            period_format=job['period_format'],
            data_to_get=job['data_to_get'],
            http_conn_id=http_conn_id,
            db_conn_id=db_conn_id
        )
