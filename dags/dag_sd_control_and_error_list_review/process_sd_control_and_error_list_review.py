import logging

from airflow.exceptions import AirflowFailException
from airflow.hooks.base import BaseHook
from airflow.models import Variable

from dag_sd_control_and_error_list_review.sd_control_and_error_list_review_data import (
    run_sd_control_and_error_list_review_job,
)

logger = logging.getLogger(__name__)


def _process_sd_control_and_error_list_review(
    department_type: str,
) -> None:
    connection = BaseHook.get_connection("sd_fleksjobrefusion_personaleweb")

    if not connection.host or not connection.login or not connection.password:
        raise AirflowFailException(
            "Connection 'sd_fleksjobrefusion_personaleweb' is missing host, username, or password"
        )

    config = Variable.get(
        "sd_control_error_list_config",
        deserialize_json=True,
    )

    department_codes = config[f"{department_type}_department_codes"]
    allowed_codes = config[f"{department_type}_allowed_codes"]

    success, _ = run_sd_control_and_error_list_review_job(
        username=connection.login,
        password=connection.password,
        sd_url=connection.host,
        department_codes=department_codes,
        allowed_codes=allowed_codes,
    )

    if not success:
        raise AirflowFailException(
            f"SD control/error review failed for {department_type}"
        )


def process_sd_control_and_error_list_review_for_ejendomservice() -> None:
    _process_sd_control_and_error_list_review("ejendomservice")


def process_sd_control_and_error_list_review_for_personale() -> None:
    _process_sd_control_and_error_list_review("personale")
