import logging

from dag_affald.affald_data import (
    fetch_affald_registration_monthly_df,
    sheet_specs_requires_carrier
)
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

logger = logging.getLogger(__name__)


def process_affald() -> None:

    logger.info("Starting Affald data processing...")

    affald_engine = MsSqlHook(mssql_conn_id="affald_sql").get_sqlalchemy_engine()

    include_carrier = sheet_specs_requires_carrier()

    affald_df = fetch_affald_registration_monthly_df(
        affald_engine=affald_engine,
        customer_names=[],
        include_carrier=include_carrier,
    )

    logger.info("Affald data processing completed successfully")
