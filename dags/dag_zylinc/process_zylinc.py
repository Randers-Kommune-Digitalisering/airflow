import logging
import pandas as pd

from airflow.hooks.base import BaseHook
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchPythonHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from dag_zylinc.zylinc_data import (
    QUEUE_NAMES,
    fetch_queue_data_from_elasticsearch,
    fetch_activity_data_from_elasticsearch,
)

logger = logging.getLogger(__name__)


def process_zylinc() -> None:
    """
    Fetch Zylinc data from Elasticsearch and load into Postgres.
    """
    conn = BaseHook.get_connection("zylinc_elasticsearch")
    host = conn.host
    port = conn.port
    user = conn.login
    password = conn.password
    scheme = conn.schema

    es_hook = ElasticsearchPythonHook(
        hosts=[f"{scheme}://{host}:{port}"],
        es_conn_args={"basic_auth": (user, password)}
    )
    es_client = es_hook.get_conn
    try:
        info = es_client.info()
        logger.info(f"Connected to Elasticsearch: {info}")
    except Exception as e:
        logger.error(f"Could not connect to Elasticsearch: {e}")

    zylinc_db_hook = PostgresHook(postgres_conn_id="zylinc_db")
    engine = zylinc_db_hook.get_sqlalchemy_engine()

    for queue_name in QUEUE_NAMES:
        logger.debug(f"Processing queue: {queue_name}")
        data_to_insert = fetch_queue_data_from_elasticsearch(es_client=es_client, queue_name=queue_name)
        if not data_to_insert:
            logger.debug(f"No data fetched for queue: {queue_name}")
            continue

        logger.debug(f"Inserting data into database for queue: {queue_name}")
        df = pd.DataFrame(data_to_insert)
        table_name = f"zylinc_{queue_name.lower()}"
        logger.debug(f"Inserting data into table: {table_name}")

        with engine.begin() as conn:
            df.to_sql(name=table_name, con=conn, if_exists='replace', index=False, chunksize=1000)
            logger.info(f"Data successfully inserted into PostgreSQL table: {table_name}")

    logger.info("Processing Activity Data")
    activity_data = fetch_activity_data_from_elasticsearch(es_client=es_client)
    if activity_data:
        df_activity = pd.DataFrame(activity_data)
        table_name = "zylinc_activity_data"
        with engine.begin() as conn:
            df_activity.to_sql(name=table_name, con=conn, if_exists='replace', index=False, chunksize=1000)
        logger.info(f"Activity Data successfully inserted into PostgreSQL table: {table_name}")
    else:
        logger.error("Error processing Activity Data")
    logger.info("Zylinc Airflow DAG completed successfully!")
