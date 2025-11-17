import logging

from airflow.utils.context import Context

logger = logging.getLogger(__name__)


def log_temperature(**context: Context) -> None:
    data = context["ti"].xcom_pull(task_ids="get_randers_temperature")
    logger.info(f'Temperature in Randers: {data["current"]["temperature_2m"]}{data["current_units"]["temperature_2m"]} at {data["current"]["time"]}')
