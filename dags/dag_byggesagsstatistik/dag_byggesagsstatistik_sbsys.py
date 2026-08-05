from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.python import PythonOperator, get_current_context
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime, timezone
from sqlalchemy.orm import Session
import logging
from datetime import datetime as dt, timedelta
from typing import Dict, List

from dag_byggesagsstatistik.models.byggesager_db_models import (
    ByggesagByg,
    ByggesagSag,
    Byggesagsgruppe,
    Byggesagskode,
    Beslutningstype,
)

from dag_byggesagsstatistik.models.randers_sbsys_models import (
    BeslutningsType,
    ByggeSag,
    ByggeSagKode,
    Sag,
    SagSkabelon,
)

from utils.config import DEFAULT_DAG_ARGS

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 2
dag_args["retry_delay"] = timedelta(minutes=30)

logger = logging.getLogger(__name__)


def sync_sbsys_to_byggesager() -> None:
    """Syncs byggesagsstatistik data from SBSYS MSSQL server to byggesager Postgres database."""
    logger.info("Starting byggesagsstatistik_sbsys job")

    context = get_current_context()
    start_date_param = context["params"].get("sync_start_date", "2020-01-01")
    try:
        sync_start_date = dt.fromisoformat(str(start_date_param))
    except ValueError as exc:
        raise AirflowFailException("Invalid param 'sync_start_date'. Expected ISO format like '2020-01-01' or '2020-01-01T00:00:00'.") from exc

    config = Variable.get("byggesagsstatistik_sbsys", default_var=None, deserialize_json=True)
    if not config:
        raise AirflowFailException("Missing Airflow Variable: byggesagsstatistik_sbsys")

    groupings = config.get("GROUPINGS")
    skabelon_ids = config.get("SKABELON_IDS")

    if not groupings or not skabelon_ids:
        raise AirflowFailException("Invalid config in Variable 'byggesagsstatistik_sbsys': GROUPINGS and SKABELON_IDS are required")

    normalized_groupings: Dict[str, List[int]] = {
        str(group_name): [int(code_id) for code_id in code_ids]
        for group_name, code_ids in groupings.items()
    }
    normalized_skabelon_ids = [int(skabelon_id) for skabelon_id in skabelon_ids]

    sbsys_engine = MsSqlHook(mssql_conn_id="sbsys-byggesager").get_sqlalchemy_engine()
    byggesager_engine = PostgresHook(postgres_conn_id="byggesager").get_sqlalchemy_engine()

    with Session(sbsys_engine) as sbsys_session, Session(byggesager_engine) as kubernetes_session:
        new_groupings = {}
        for key, code_ids in normalized_groupings.items():
            dist = kubernetes_session.query(Byggesagsgruppe).filter_by(name=key).first()
            if not dist:
                dist = Byggesagsgruppe(name=key)
                kubernetes_session.add(dist)
                kubernetes_session.flush()
            new_groupings[dist.id] = code_ids

        def get_grouping_id(code_id: int):
            return next((group_id for group_id, code_list in new_groupings.items() if code_id in code_list), None)

        logger.info("Syncing metadata tables from SBSYS")
        for orig in sbsys_session.query(BeslutningsType).all():
            kubernetes_session.merge(Beslutningstype(id=orig.ID, name=orig.Navn))

        for orig in sbsys_session.query(ByggeSagKode).all():
            kubernetes_session.merge(
                Byggesagskode(
                    id=orig.ID,
                    byggesagsgruppe_id=get_grouping_id(orig.ID),
                    name=orig.Kode,
                )
            )

        for orig in sbsys_session.query(SagSkabelon).filter(SagSkabelon.ID.in_(normalized_skabelon_ids)).all():
            kubernetes_session.merge(
                Byggesagskode(
                    id=orig.ID,
                    byggesagsgruppe_id=get_grouping_id(orig.ID),
                    name=orig.Navn,
                )
            )

        logger.info("Syncing byggesag rows from SBSYS")
        for orig in sbsys_session.query(ByggeSag).filter(ByggeSag.Modtaget >= sync_start_date).all():
            if orig.ByggeSagKodeID:
                kubernetes_session.merge(
                    ByggesagByg(
                        id=orig.ID,
                        byggesagskode_id=orig.ByggeSagKodeID,
                        beslutningstype_id=orig.Sag.BeslutningsTypeID if orig.Sag else None,
                        byggetilladelse_date=orig.Byggetilladelse,
                        received_date=orig.Modtaget,
                    )
                )

        for orig in sbsys_session.query(Sag).filter(
            Sag.SkabelonID.in_(normalized_skabelon_ids),
            Sag.Created >= sync_start_date,
        ).all():
            kubernetes_session.merge(
                ByggesagSag(
                    id=orig.ID,
                    byggesagskode_id=orig.SkabelonID,
                    beslutningstype_id=orig.BeslutningsTypeID,
                    byggetilladelse_date=orig.LastStatusChange,
                    received_date=orig.Created,
                )
            )

        logger.info("Committing changes to byggesager Postgres DB")
        kubernetes_session.commit()

    logger.info("byggesagsstatistik_sbsys job completed successfully")


with DAG(
    dag_id="byggesagsstatistik_sbsys_data_to_db",
    start_date=datetime(2026, 8, 5, tz=timezone("Europe/Copenhagen")),
    schedule="@monthly",
    catchup=False,
    params={
        "sync_start_date": Param(
            "2020-01-01",
            type="string",
            format="date",
            description="Lower bound date for SBSYS records to sync (YYYY-MM-DD).",
        )
    },
    default_args=dag_args,
    description="Sync byggesagsstatistik data from SBSYS MSSQL server to byggesager Postgres",
    tags=["mssql", "postgres", "sbsys", "byggesager", "data", "sync", "db", "database"],
) as dag:

    sync_sbsys_data = PythonOperator(
        task_id="sync_sbsys_data",
        python_callable=sync_sbsys_to_byggesager,
        do_xcom_push=False
    )

    sync_sbsys_data
