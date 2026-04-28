import logging

from datetime import datetime
from typing import Tuple

from sqlalchemy.orm import Session
from sqlalchemy import and_, text  # , or_
from sqlalchemy.orm import selectinload
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

from dag_sbsys_luk.model import (
    CivilstandOpslag,
    Dokument,
    DokumentRegistrering,
    KladdeRegistrering,
    Person,
    Sag,
    Kladde,
    Sagspart,
    Sagsstatus,
)

logger = logging.getLogger(__name__)

USER_ID = 9999  # Placeholder for the user ID performing the closure
SAG_STATUS_CLOSED_PROD = 5


def _iter_dokument_shard_dbs(session: Session) -> list[str]:
    return session.execute(
        text(
            """
            SELECT name
            FROM sys.databases
            WHERE name LIKE 'SbsysNetDriftDokument[0-9][0-9][0-9][0-9]'
            ORDER BY name
            """
        )
    ).scalars().all()


def _iter_kladde_shard_dbs(session: Session) -> list[str]:
    return session.execute(
        text(
            """
            SELECT name
            FROM sys.databases
            WHERE name LIKE 'SbsysNetDriftKladde[0-9][0-9][0-9][0-9]'
            ORDER BY name
            """
        )
    ).scalars().all()


def _get_newest_shard_db(shard_dbs: list[str], shard_type_label: str) -> str:
    if not shard_dbs:
        raise ValueError(f"No shard databases found for {shard_type_label}.")
    return max(shard_dbs)


def _get_kladde_data_blob(session: Session, kladde_shard_dbs: list[str], kladde_id: int) -> Tuple[str, bytes] | None:
    for shard_db in kladde_shard_dbs:
        blob = session.execute(
            text(
                f"""
                SELECT TOP (1) Data
                FROM [{shard_db}].dbo.KladdeData
                WHERE KladdeID = :kladde_id
                """
            ),
            {"kladde_id": kladde_id},
        ).scalar_one_or_none()

        if blob is not None:
            return shard_db, blob

    return None


def _delete_kladde_data(session: Session, shard_db: str, kladde_id: int) -> int:
    result = session.execute(
        text(
            f"""
            DELETE FROM [{shard_db}].dbo.KladdeData
            WHERE KladdeID = :kladde_id
            """
        ),
        {"kladde_id": kladde_id},
    )
    return result.rowcount or 0


def _insert_dokument_data(session: Session, shard_db: str, dokument_id: int, data: bytes) -> None:
    session.execute(
        text(
            f"""
            INSERT INTO [{shard_db}].dbo.DokumentData (DokumentID, Data)
            VALUES (:dokument_id, :data)
            """
        ),
        {"dokument_id": dokument_id, "data": data},
    )


def process_sbsys_luk(required_sagsstatus: list, required_sagsskabelon_ids: list, ignore_sagsskabelon_ids: list, dry_run: bool) -> None:
    """
    Fetch and close SBSYS cases based on specific criteria using SQL.
    """
    hook = MsSqlHook(mssql_conn_id="sbsys_luk_prod")
    engine = hook.get_sqlalchemy_engine()

    with Session(engine) as session:
        dokument_shard_dbs = _iter_dokument_shard_dbs(session)
        kladde_shard_dbs = _iter_kladde_shard_dbs(session)
        newest_dokument_shard_db = _get_newest_shard_db(dokument_shard_dbs, shard_type_label="DokumentData")
        logger.info(f"Newest DokumentData shard selected for inserts: {newest_dokument_shard_db}")

        # Query to fetch cases that match the criteria
        query = (
            session.query(Sag)
            .options(
                selectinload(Sag.Erindring),
                selectinload(Sag.KladdeRegistrering).selectinload(KladdeRegistrering.Kladde),
            )
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
                erindring.Afsluttet = datetime.now()
                erindring.AfsluttetAfID = USER_ID
                erindring.AfsluttetNotat = "Erindring afsluttet ifm. automatisk sagslukning af robot (Digitalisering)."
                session.add(erindring)

            # Journalize all KladdeRegistrering records associated with the case
            for kladde_reg in sag.KladdeRegistrering:
                kladde: Kladde | None = kladde_reg.Kladde
                if kladde is None:
                    logger.warning(f"KladdeRegistrering ID {kladde_reg.ID} has no related Kladde; skipping")
                    continue

                if kladde.IsArchived:
                    logger.info(f"Kladde ID {kladde.ID} already archived; skipping")
                    continue

                logger.info(f"Journalizing KladdeRegistrering ID {kladde_reg.ID} (Kladde ID {kladde.ID}) for case ID {sag.ID}")

                kladde_data_info = _get_kladde_data_blob(session, kladde_shard_dbs, kladde.ID)
                if kladde_data_info is None:
                    logger.warning(f"No KladdeData found for Kladde ID {kladde.ID}; skipping")
                    continue

                kladde_data_shard_db, kladde_blob = kladde_data_info

                if dry_run:
                    logger.info(
                        "DRY_RUN: Would create Dokument/DokumentRegistrering, copy blob "
                        f"from {kladde_data_shard_db} to {newest_dokument_shard_db}, archive Kladde and delete KladdeData"
                    )
                    continue

                dokument = Dokument(
                    Navn=kladde.Navn,
                    Beskrivelse=kladde.Beskrivelse,
                    FraKladdeID=kladde.ID,
                )
                session.add(dokument)
                session.flush()

                dokument_reg = DokumentRegistrering(
                    SagID=sag.ID,
                    DokumentID=dokument.ID,
                    Navn=kladde_reg.Navn,
                    Beskrivelse=kladde_reg.Beskrivelse,
                )
                session.add(dokument_reg)
                session.flush()

                # Keep FK pair consistent if the schema expects Dokument -> DokumentRegistrering.
                dokument.DokumentRegistreringID = dokument_reg.ID
                session.flush()

                _insert_dokument_data(session, newest_dokument_shard_db, dokument.ID, kladde_blob)

                kladde.IsArchived = 1
                session.add(kladde)

                deleted = _delete_kladde_data(session, kladde_data_shard_db, kladde.ID)
                logger.info(f"Deleted {deleted} KladdeData row(s) for Kladde ID {kladde.ID} from shard {kladde_data_shard_db}")

            # Update the case status to 'Lukket'
            sag.SagsStatusID = SAG_STATUS_CLOSED_PROD
            sag.LastStatusChange = datetime.now()
            sag.LastStatusChangeComments = "Sagsstatus ændret til 'Lukket' ifm. automatisk sagslukning af robot (Digitalisering)."
            session.add(sag)

        # Commit all changes to the database
        if not dry_run:
            session.commit()
            logger.info("All identified cases have been closed and changes committed to the database.")
        else:
            logger.info("DRY_RUN is enabled. No changes have been committed to the database.")

    return None
