import logging

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowFailException

from dag_asset.asset_data import (
    create_asset_tables,
    insert_department_ean_from_delta,
    insert_departments_data,
    insert_users_data,
    insert_computers_data,
    insert_atea_data,
    upload_assets_to_topdesk,
    insert_device_license_and_historical_data,
)
from rkdigi.syncpkg.token_session import ManagedOAuth2Session

logger = logging.getLogger(__name__)


def task_create_asset_tables() -> None:
    asset_engine = PostgresHook(postgres_conn_id="asset_db").get_sqlalchemy_engine()
    if not create_asset_tables(db_engine=asset_engine):
        raise AirflowFailException("Failed to create asset tables")


def task_insert_departments_data() -> None:
    capa_cms_engine = MsSqlHook(mssql_conn_id="capa_cms_db").get_sqlalchemy_engine()
    asset_engine = PostgresHook(postgres_conn_id="asset_db").get_sqlalchemy_engine()

    if not insert_departments_data(capa_cms_engine=capa_cms_engine, asset_engine=asset_engine):
        raise AirflowFailException("Failed to insert departments data")


def task_insert_department_ean_from_delta() -> None:
    asset_engine = PostgresHook(postgres_conn_id="asset_db").get_sqlalchemy_engine()
    delta_hook = BaseHook.get_connection("delta_prod")

    delta_token_session = ManagedOAuth2Session(
        token_url=delta_hook.extra_dejson.get("token_url"),
        client_id=delta_hook.login,
        client_secret=delta_hook.password,
    )

    if not insert_department_ean_from_delta(
        token_session=delta_token_session,
        asset_engine=asset_engine,
        delta_base_url=delta_hook.host,
    ):
        raise AirflowFailException("Failed to insert department EANs from Delta")


def task_insert_users_data() -> None:
    capa_cms_engine = MsSqlHook(mssql_conn_id="capa_cms_db").get_sqlalchemy_engine()
    asset_engine = PostgresHook(postgres_conn_id="asset_db").get_sqlalchemy_engine()

    if not insert_users_data(capa_cms_engine=capa_cms_engine, asset_engine=asset_engine):
        raise AirflowFailException("Failed to insert users data")


def task_insert_computers_data() -> None:
    capa_cms_engine = MsSqlHook(mssql_conn_id="capa_cms_db").get_sqlalchemy_engine()
    asset_engine = PostgresHook(postgres_conn_id="asset_db").get_sqlalchemy_engine()

    if not insert_computers_data(capa_cms_engine=capa_cms_engine, asset_engine=asset_engine):
        raise AirflowFailException("Failed to insert computers data")


def task_insert_atea_data() -> None:
    asset_engine = PostgresHook(postgres_conn_id="asset_db").get_sqlalchemy_engine()
    atea_http_hook = HttpHook(http_conn_id="atea_api")

    if not insert_atea_data(http_hook=atea_http_hook, asset_engine=asset_engine):
        raise AirflowFailException("Failed to insert Atea data")


def task_insert_device_license_and_historical_data() -> None:
    asset_engine = PostgresHook(postgres_conn_id="asset_db").get_sqlalchemy_engine()
    atea_http_hook = HttpHook(http_conn_id="atea_api")
    asset_sftp_hook = SFTPHook(ssh_conn_id="asset_sftp")

    if not insert_device_license_and_historical_data(
        sftp_hook=asset_sftp_hook,
        http_hook=atea_http_hook,
        asset_engine=asset_engine,
    ):
        raise AirflowFailException("Failed to insert device license and historical data")


def task_upload_assets_to_topdesk() -> None:
    asset_engine = PostgresHook(postgres_conn_id="asset_db").get_sqlalchemy_engine()
    topdesk_http_hook = HttpHook(http_conn_id="topdesk_api_test")

    if not upload_assets_to_topdesk(asset_engine=asset_engine, http_hook=topdesk_http_hook):
        raise AirflowFailException("Failed to upload assets to TopDesk")
