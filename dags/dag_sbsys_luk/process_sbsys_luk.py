import datetime
import logging

from sqlalchemy.orm import Session
from sqlalchemy import and_, or_
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

from dag_sbsys_luk.model import CivilstandOpslag, Person, Sag, Sagspart, Sagsstatus

logger = logging.getLogger(__name__)

USER_ID = 9999  # Placeholder for the user ID performing the closure
SAG_STATUS_CLOSED_PROD = 5


def process_sbsys_luk(sagsskabelon_ids: list, dry_run: bool) -> None:
    """
    Fetch and close SBSYS cases based on specific criteria using SQL.
    """
    hook = MsSqlHook(mssql_conn_id="sbsys_luk_prod")
    engine = hook.get_sqlalchemy_engine()

    seen_sag_ids = set()
    sager_to_close = []

    # Query to fetch cases that match the specified SkabelonIDs
    with Session(engine) as session:
        result = (
            session.query(Sag)
            .filter(
                Sag.SkabelonID.in_(sagsskabelon_ids)
            )
            .all()
        )

        logger.info(f"Fetched {len(result)} cases with SkabelonID in {sagsskabelon_ids}")
        for sag in result:
            if sag.ID not in seen_sag_ids:
                seen_sag_ids.add(sag.ID)
                sager_to_close.append(sag)

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
        for sag in result:
            if sag.ID not in seen_sag_ids:
                seen_sag_ids.add(sag.ID)
                sager_to_close.append(sag)

    logger.info(f"Total cases identified for closure: {len(sager_to_close)}")

    # Close the cases that were identified
    with Session(engine) as session:
        for sag in sager_to_close:
            logger.info(f"Closing case ID {sag.ID} with SkabelonID {sag.SkabelonID}")

            # Complete all Erindring records associated with the case
            for erindring in sag.Erindring:
                erindring.ErAfsluttet = 1
                erindring.AfsluttetDato = datetime.now()
                erindring.AfsluttetAfID = USER_ID
                erindring.AfsluttetNotat = "Erindring afsluttet ifm. automatisk sagslukning af robot (Digitalisering)."
                session.add(erindring)

            # Delete all KladdeRegistrering records associated with the case
            for kladde in sag.Kladde:
                kladde.DeletedState = 1
                kladde.DeletedDate = datetime.now()
                kladde.DeletedByID = USER_ID
                kladde.DeletedReason = "Kladde registrering slettet ifm. automatisk sagslukning af robot (Digitalisering)."
                kladde.DeleteConfirmed = datetime.now()
                kladde.DeleteConfirmedByID = USER_ID
                session.add(kladde)

            # Update the case status to 'Lukket'
            sag.SagsStatusID = SAG_STATUS_CLOSED_PROD
            sag.LastStatusChange = datetime.now()
            sag.LastStatusChangeComment = "Sagsstatus ændret til 'Lukket' ifm. automatisk sagslukning af robot (Digitalisering)."
            session.add(sag)

        # Commit all changes to the database
        if not dry_run:
            session.commit()
            logger.info("All identified cases have been closed and changes committed to the database.")
        else:
            logger.info("DRY_RUN is enabled. No changes have been committed to the database.")

    return None
