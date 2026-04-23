import logging

from datetime import datetime

from sqlalchemy.orm import Session
from sqlalchemy import and_  # , or_
from sqlalchemy.orm import selectinload
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

from dag_sbsys_luk.model import CivilstandOpslag, Person, Sag, Sagspart, Sagsstatus

logger = logging.getLogger(__name__)

USER_ID = 9999  # Placeholder for the user ID performing the closure
SAG_STATUS_CLOSED_PROD = 5


def process_sbsys_luk(required_sagsstatus: list, required_sagsskabelon_ids: list, ignore_sagsskabelon_ids: list, dry_run: bool) -> None:
    """
    Fetch and close SBSYS cases based on specific criteria using SQL.
    """
    hook = MsSqlHook(mssql_conn_id="sbsys_luk_prod")
    engine = hook.get_sqlalchemy_engine()

    with Session(engine) as session:
        # Query to fetch cases that match the criteria
        query = (
            session.query(Sag)
            .options(selectinload(Sag.Erindring), selectinload(Sag.Kladde))
            .filter(
                Sag.SagsStatus.has(Sagsstatus.Navn.in_(required_sagsstatus)),
                Sag.SkabelonID.notin_(ignore_sagsskabelon_ids),
                Sag.SagsPart.has(
                    and_(
                        Sagspart.PartType == 1,
                        Sagspart.Person.has(Person.Civilstand.has(CivilstandOpslag.Navn == "Død")),
                    )
                ),
            )
        )

        # Apply additional filter for required SkabelonIDs if provided
        if required_sagsskabelon_ids:
            query = query.filter(Sag.SkabelonID.in_(required_sagsskabelon_ids))

        sager_to_close = query.all()
        logger.info(f"Found {len(sager_to_close)} cases to close based on the specified criteria.")

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
            # # TODO: SKAL JOURNALISERES I STEDET FOR!
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
