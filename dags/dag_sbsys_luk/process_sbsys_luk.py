import logging

from sqlalchemy.orm import Session
from sqlalchemy import and_, or_
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

from dags.dag_sbsys_luk.model import CivilstandOpslag, Person, Sag, Sagspart, Sagsstatus

logger = logging.getLogger(__name__)
SAGSSKABELON_IDS = [5133]


def process_sbsys_luk() -> None:
    """
    Fetch and close SBSYS cases based on specific criteria using SQL.
    """

    hook = MsSqlHook(mssql_conn_id="your_mssql_connection_id")
    engine = hook.get_sqlalchemy_engine()

    # Query to fetch cases that match the specified SkabelonIDs
    with Session(engine) as session:
        result = (
            session.query(Sag)
            .filter(
                Sag.SkabelonID.in_(SAGSSKABELON_IDS)
            )
            .all()
        )
        logger.info(f"Fetched {len(result)} cases with SkabelonID in {SAGSSKABELON_IDS}")

    # Query to fetch cases where primary part has deceased
    with Session(engine) as session:
        result = (
            session.query(Sag)
            .filter(
                Sag.SagsStatus.has(or_(Sagsstatus.Navn == 'Aktiv', Sagsstatus.Navn == 'Opstået')),
                Sag.SagsPart.has(
                    and_(
                        Sagspart.PartType == 1,
                        Sagspart.Person.has(
                            Person.Civilstand.has(CivilstandOpslag.Navn == 'Død')
                        ),
                    )
                ),
            )
            .all()
        )
        logger.info(f"Found {len(result)} cases with deceased primary part.")
