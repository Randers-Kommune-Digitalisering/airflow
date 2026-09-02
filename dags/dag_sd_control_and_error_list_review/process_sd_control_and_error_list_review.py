import logging

from airflow.exceptions import AirflowFailException
from airflow.hooks.base import BaseHook
from airflow.models import Variable

from dag_sd_control_and_error_list_review.sd_control_and_error_list_review_data import (
    run_sd_control_and_error_list_review_job,
)

logger = logging.getLogger(__name__)


def process_sd_control_and_error_list_review() -> None:
    """
    Review SD control and error messages for the configured departments.
    """
    logger.info("Starting to process sd_control_and_error_list_review data...")
    sd_personaleweb = BaseHook.get_connection("sd_fleksjobrefusion_personaleweb")
    host = sd_personaleweb.host
    username = sd_personaleweb.login
    password = sd_personaleweb.password
    if not username or not password or not host:
        raise AirflowFailException(
            "Connection 'sd_fleksjobrefusion_personaleweb' is missing host, username, or password"
        )

    sd_control_error_list_config = Variable.get("sd_control_error_list_config", deserialize_json=True)
    department_codes = sd_control_error_list_config["department_codes"]
    allowed_codes = sd_control_error_list_config["allowed_codes"]

    run_sd_control_and_error_list_review_job(
        username=username,
        password=password,
        department_codes=department_codes,
        allowed_codes=allowed_codes,
        sd_url=host,
    )
