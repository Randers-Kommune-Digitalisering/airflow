import logging
import requests

from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import MetaData, Table, select, update
from sqlalchemy.engine import Engine

from dag_xflow_nexus_hjaelpemidler.handlers import HANDLERS
from dag_xflow_nexus_hjaelpemidler.nexus import NexusClient
from dag_xflow_nexus_hjaelpemidler.models import SCHEMA, RowStatus


logger = logging.getLogger(__name__)


def get_xflow_data_add_to_nexus(tables: list[str], meta_hook: PostgresHook, nexus_hook: BaseHook, xflow_hook: BaseHook, var_name: str) -> None:
    """ Process every received row in the given xFlow tables and send it to Nexus. """
    nexus_client = NexusClient(nexus_hook=nexus_hook)
    xflow_conn = xflow_hook.get_connection(xflow_hook.http_conn_id)
    meta_engine = meta_hook.get_sqlalchemy_engine()
    metadata = MetaData(schema=SCHEMA)

    with requests.Session() as xflow_session:
        xflow_session.headers.update({"publicApiToken": xflow_conn.password})

        failed_rows: dict[str, list[int]] = {}
        for table_name in tables:
            if table_name not in HANDLERS:
                raise ValueError(f"Unknown table '{table_name}' specified in '{var_name}' variable.")
            table = Table(table_name, metadata, autoload_with=meta_engine)
            failed_row_ids = process_table(engine=meta_engine, table=table, handler=HANDLERS[table_name], nexus_client=nexus_client, xflow_session=xflow_session)
            if failed_row_ids:
                failed_rows[table_name] = failed_row_ids

    if failed_rows:
        raise RuntimeError(f"Failed processing rows: {failed_rows}")


def process_table(engine: Engine, table: Table, handler, nexus_client: NexusClient, xflow_session: requests.Session) -> list[int]:
    """ Handle one row at a time until no received rows are left and return the ids that failed. """
    failed_row_ids: list[int] = []
    while True:
        with engine.begin() as conn:
            row = conn.execute(
                select(table)
                .where(table.c.status == RowStatus.RECEIVED.value)
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
                .values(status=RowStatus.PROCESSING.value)
            )

        try:
            handler(
                nexus_client=nexus_client,
                xflow_session=xflow_session,
                row=row,
                table_name=table.name,
            )
        except Exception:
            logger.exception(f"Failed processing {table.name} id={row_id}")
            with engine.begin() as conn:
                conn.execute(update(table).where(table.c.id == row_id).values(status=RowStatus.FAILED.value))
            failed_row_ids.append(row_id)
            continue

        with engine.begin() as conn:
            conn.execute(
                update(table)
                .where(table.c.id == row_id)
                .values(status=RowStatus.SUCCESS.value)
            )
