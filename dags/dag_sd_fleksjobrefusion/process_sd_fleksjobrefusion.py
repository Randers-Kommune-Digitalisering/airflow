import logging

from airflow.exceptions import AirflowFailException
from airflow.hooks.base import BaseHook

from dag_sd_fleksjobrefusion.sd_fleksjobrefusion_data import (
    run_sd_fleksjobrefusion_login,
)

logger = logging.getLogger(__name__)


def process_sd_fleksjobrefusion() -> None:
    """
    Run SD Fleksjobrefusion login flow through Playwright.
    """
    logger.info("Starting SD Fleksjobrefusion processing")

    sd_fleksjobrefusion = BaseHook.get_connection("sd_fleksjobrefusion")

    username = sd_fleksjobrefusion.login
    password = sd_fleksjobrefusion.password
    if not username or not password:
        raise AirflowFailException("Connection 'sd_fleksjobrefusion' is missing username and password")

    success = run_sd_fleksjobrefusion_login(
        username=username,
        password=password,
    )

    if not success:
        raise AirflowFailException("SD Fleksjobrefusion login flow failed")

    logger.info("SD Fleksjobrefusion login flow processing completed successfully")
