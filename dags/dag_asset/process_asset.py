import logging

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.http.hooks.http import HttpHook

from dag_asset.asset_data import (
    create_asset_tables,
    insert_departments_data,
    insert_users_data,
    insert_computers_data,
    insert_atea_data
)

logger = logging.getLogger(__name__)


def process_assets() -> None:
    """
    Load computer asset data from CAPA into Postgres Asset DB
    """
    atea_http_hook = HttpHook(http_conn_id="atea_api")
    capa_cms_db_hook = MsSqlHook(mssql_conn_id="capa_cms_db")
    asset_db_hook = PostgresHook(postgres_conn_id="asset_db")

    capa_cms_engine = capa_cms_db_hook.get_sqlalchemy_engine()
    asset_engine = asset_db_hook.get_sqlalchemy_engine()

    if not create_asset_tables(db_engine=asset_engine):
        raise ValueError("Failed to create asset tables")

    if not insert_departments_data(
        capa_cms=capa_cms_engine,
        asset_engine=asset_engine
    ):
        raise ValueError("Failed to insert departments data")

    if not insert_users_data(
        capa_cms=capa_cms_engine,
        asset_engine=asset_engine
    ):
        raise ValueError("Failed to insert users data")

    if not insert_computers_data(
        capa_cms=capa_cms_engine,
        asset_engine=asset_engine
    ):
        raise ValueError("Failed to insert computers data")

    if not insert_atea_data(
        http_hook=atea_http_hook,
        asset_engine=asset_engine
    ):
        raise ValueError("Failed to insert Atea data")

    logger.info("Asset ETL completed successfully")
