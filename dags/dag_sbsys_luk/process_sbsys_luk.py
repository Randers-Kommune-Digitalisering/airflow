import logging

from datetime import datetime

from sqlalchemy.orm import Session
from sqlalchemy import and_, or_
from sqlalchemy.orm import selectinload
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

    with Session(engine) as session:
        sag_ids_to_close: set[int] = set()

        # Query to fetch cases that match the specified SkabelonIDs
        skabelon_sag_ids = (
            session.query(Sag.ID)
            .filter(Sag.SkabelonID.in_(sagsskabelon_ids))
            .all()
        )
        sag_ids_to_close.update(sag_id for (sag_id,) in skabelon_sag_ids)
        logger.info(
            "Fetched %s cases with SkabelonID in %s",
            len(skabelon_sag_ids),
            sagsskabelon_ids,
        )

        # Query to fetch cases where primary part has deceased
        deceased_sag_ids = (
            session.query(Sag.ID)
            .filter(
                Sag.SagsStatus.has(or_(Sagsstatus.Navn == "Aktiv", Sagsstatus.Navn == "Opstået")),
                Sag.SagsPart.has(
                    and_(
                        Sagspart.PartType == 1,
                        Sagspart.Person.has(
                            Person.Civilstand.has(CivilstandOpslag.Navn == "Død")
                        ),
                    )
                ),
            )
            .all()
        )
        sag_ids_to_close.update(sag_id for (sag_id,) in deceased_sag_ids)
        logger.info("Found %s cases with deceased primary part.", len(deceased_sag_ids))

        logger.info("Total cases identified for closure: %s", len(sag_ids_to_close))
        if not sag_ids_to_close:
            return None

        # Re-load the cases inside this same session with relationships eagerly loaded.
        # This avoids DetachedInstanceError when accessing sag.Erindring / sag.Kladde.
        sager_to_close = (
            session.query(Sag)
            .options(selectinload(Sag.Erindring), selectinload(Sag.Kladde))
            .filter(Sag.ID.in_(sorted(sag_ids_to_close)))
            .all()
        )

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
