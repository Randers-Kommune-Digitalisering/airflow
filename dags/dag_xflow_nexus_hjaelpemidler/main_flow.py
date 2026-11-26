import logging
import requests

from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import MetaData, Table, inspect, select, update
from sqlalchemy.engine import Engine

from dag_xflow_nexus_hjaelpemidler.config import FORM_CONFIG_BY_TABLE
from dag_xflow_nexus_hjaelpemidler.nexus import NexusClient
from dag_xflow_nexus_hjaelpemidler.factory import get_nexus_case
from dag_xflow_nexus_hjaelpemidler.handlers import process_nexus_case
from dag_xflow_nexus_hjaelpemidler.models import SCHEMA, HjaelpemiddelStatus


logger = logging.getLogger(__name__)


def get_xflow_data_add_to_nexus(meta_hook: PostgresHook, nexus_hook: BaseHook, xflow_hook: BaseHook, tables: list[str]) -> None:
    """ Process every received row in the given xFlow tables and send it to Nexus. """
    nexus_client = NexusClient(nexus_hook=nexus_hook)
    xflow_conn = xflow_hook.get_connection(xflow_hook.http_conn_id)
    meta_engine = meta_hook.get_sqlalchemy_engine()
    metadata = MetaData(schema=SCHEMA)
    inspector = inspect(meta_engine)

    with requests.Session() as xflow_session:
        xflow_session.headers.update({"publicApiToken": xflow_conn.password})

        failed_rows: dict[str, list[int]] = {}
        for table_name in tables:
            if not inspector.has_table(table_name, schema=SCHEMA):
                raise ValueError(
                    f"Table '{SCHEMA}.{table_name}' does not exist."
                )
            if table_name not in FORM_CONFIG_BY_TABLE:
                raise ValueError(f"No Nexus form configuration registered for table '{table_name}'")
            table = Table(table_name, metadata, autoload_with=meta_engine)
            failed_row_ids = process_table(engine=meta_engine, table=table, nexus_client=nexus_client, xflow_session=xflow_session)
            if failed_row_ids:
                failed_rows[table_name] = failed_row_ids

    if failed_rows:
        raise RuntimeError(f"Failed processing rows: {failed_rows}")


def process_table(engine: Engine, table: Table, nexus_client: NexusClient, xflow_session: requests.Session) -> list[int]:
    """ Handle one row at a time until no received rows are left and return the ids that failed. """
    failed_row_ids: list[int] = []
    while True:
        with engine.begin() as conn:
            row = conn.execute(
                select(table)
                .where(table.c.status == HjaelpemiddelStatus.RECEIVED.value)
                .order_by(table.c.id)
                .limit(1)
                .with_for_update(skip_locked=True)
            ).one_or_none()

            if row is None:
                return failed_row_ids

            row_id = row.id
            conn.execute(
                update(table)
                .where(table.c.id == row_id)
                .values(status=HjaelpemiddelStatus.PROCESSING.value)
            )

        logger.info(f"Processing {table.name} id: {row_id}")
        try:
            nexus_case = get_nexus_case(
                session=xflow_session,
                table_name=table.name,
                model_object=row,
            )
            process_nexus_case(
                nexus_client=nexus_client,
                xflow_session=xflow_session,
                nexus_case=nexus_case
            )
        except Exception:
            logger.exception(f"Failed processing {table.name} id={row_id}")
            with engine.begin() as conn:
                conn.execute(update(table).where(table.c.id == row_id).values(status=HjaelpemiddelStatus.FAILED.value))
            logger.info(f"Marked {table.name} id: {row_id} as FAILED")
            failed_row_ids.append(row_id)
            continue

        with engine.begin() as conn:
            conn.execute(
                update(table)
                .where(table.c.id == row_id)
                .values(status=HjaelpemiddelStatus.SUCCESS.value)
            )
        logger.info(f"Marked {table.name} id: {row_id} as SUCCESS")
