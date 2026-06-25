import logging
import pandas as pd
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from dag_vognpark.vognpark_data import (
    INSUBIZ_EXCEL_FIELDS,
    fetch_insubiz_customers,
    fetch_insubiz_vehicles,
    normalize_insubiz_df,
    enrich_vehicles_with_customer_levels,
)
from airflow.operators.python import get_current_context

logger = logging.getLogger(__name__)


def process_vognpark_insubiz_data_to_db() -> None:
    """
    Placeholder function for processing the vognpark_insubiz_data_to_db data.
    """
    insubiz_hook = HttpHook(http_conn_id="insubiz_cloud_api")
    vognpark_hook = PostgresHook(postgres_conn_id="vognpark_db")

    vehicles = fetch_insubiz_vehicles(http_hook=insubiz_hook)
    insubiz_df = pd.json_normalize(vehicles, sep="_")

    customers = fetch_insubiz_customers(http_hook=insubiz_hook)
    insubiz_df = enrich_vehicles_with_customer_levels(vehicles_df=insubiz_df, customers=customers)

    insubiz_df = insubiz_df.reindex(columns=INSUBIZ_EXCEL_FIELDS)
    insubiz_df = normalize_insubiz_df(df=insubiz_df)

    # get current context to get logical date and timezone for report_date
    ctx = get_current_context()
    logical_date = ctx["logical_date"]
    dag_tz = ctx["dag"].timezone
    report_date = logical_date.in_timezone(dag_tz).date().isoformat()

    engine = vognpark_hook.get_sqlalchemy_engine()

    with engine.begin() as conn:
        insubiz_df.to_sql("vognpark_data", con=conn, if_exists="replace", index=False)

        # latest report date is stored in audit table
        audit_df = pd.DataFrame({"report_date": [report_date]})
        audit_df.to_sql("vognpark_run_audit", con=conn, if_exists="replace", index=False)

    logger.info("Vognpark data processed successfully")
