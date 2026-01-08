import asyncio
import logging
import pandas as pd

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.azure.hooks.msgraph import KiotaRequestAdapterHook
from airflow.hooks.base import BaseHook
from msgraph.graph_service_client import GraphServiceClient

from dag_sharepoint.sharepoint_data import (
    ms_graph_get_sharepoint_list_items_async,
    transform_sharepoint_items,
    SHAREPOINT_FIELDS,
)

logger = logging.getLogger(__name__)


def process_sharepoint_list_items() -> None:

    conn = BaseHook.get_connection("sharepoint_handleplan_config")
    sharepoint_config = conn.extra_dejson
    site_id = sharepoint_config.get("site_id")
    list_id = sharepoint_config.get("list_id")

    ms_graph_hook = KiotaRequestAdapterHook(conn_id="ms_graph_sharepoint_handleplan")
    ms_graph_adapter = ms_graph_hook.get_conn()
    ms_graph_client = GraphServiceClient(request_adapter=ms_graph_adapter)

    sharepoint_db_hook = PostgresHook(postgres_conn_id="sharepoint_db")
    sharepoint_db_engine = sharepoint_db_hook.get_sqlalchemy_engine()

    async def _fetch_transform_and_save_sharepoint_items() -> None:
        """Fetch, transform, and save SharePoint items asynchronously."""
        items = await ms_graph_get_sharepoint_list_items_async(
            client=ms_graph_client,
            site_id=site_id,
            list_id=list_id,
            fields=SHAREPOINT_FIELDS,
        )

        transformed_items = transform_sharepoint_items(items)

        if not transformed_items:
            logger.warning("No SharePoint data found.")
            return

        sharepoint_df = pd.DataFrame(transformed_items)

        logger.info(
            f"Found {len(sharepoint_df)} items in SharePoint List Handleplan – T&D strategi"
        )

        with sharepoint_db_engine.begin() as conn:
            sharepoint_df.to_sql(
                name="sharepoint_handleplan_items",
                con=conn,
                if_exists="replace",
                index=False,
            )

        logger.info(
            "SharePoint data successfully fetched, processed, and saved into DB"
        )

    asyncio.run(_fetch_transform_and_save_sharepoint_items())
