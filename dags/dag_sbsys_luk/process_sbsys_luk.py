import logging

from datetime import datetime
from typing import Tuple

from sqlalchemy.orm import Session
from sqlalchemy import and_, text  # , or_
from sqlalchemy.orm import selectinload
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.models import Variable
from rkdigi import DatabaseManager

from dag_sbsys_luk.model import (
    CivilstandOpslag,
    Dokument,
    DokumentRegistrering,
    DokumentDataInfo,
    DelforloebDokumentRegistrering,
    KladdeRegistrering,
    Person,
    Sag,
    Kladde,
    Sagspart,
    Sagsstatus,
)

logger = logging.getLogger(__name__)
ENV = "Test" if Variable.get("SBSYS_LUK_TEST_ENV", default_var="False").lower() == "true" else "Drift"

USER_ID = 202653  # User: "Autoafslutsag"
# TODO: Consider dynamic appraoch instead of hard code values for SAG_STATUS_CLOSED
SAG_STATUS_CLOSED = 8 if ENV == "Test" else 5  # 5 corresponds to 'Lukket' in production, 8 in test
DOKUMENT_ART_ID = 6  # Dokumentart 6: "Andet"
DOKUMENT_TYPE_ID = 0  # Dokumenttype 0: "Uspecificeret"
DOKUMENT_DATA_INFO_TYPE_ID = 2  # Data type info 2: "Unspecified"

def _get_dokument_data_type(fileExtension: str) -> int:
    """
    Map a file extension to a SBSYS DokumentDataType ID.

    Notes:
    - Expects a file extension including the leading dot (e.g. ".pdf").
    - The mapping is domain-specific and should be kept in sync with SBSYS' lookup values.

    :param fileExtension: File extension including leading dot (e.g. ".pdf")
    :return: Integer DokumentDataType ID; defaults to 0 (unknown) when unmapped
    """
    extension_mapping = {
        # Word Document
        ".doc": 1,
        ".docx": 1,
        # Excel Spreadsheet
        ".xls": 2,
        ".xlsx": 2,
        # PowerPoint Presentation
        ".ppt": 3,
        ".pptx": 3,
        # Text Document
        ".txt": 4,
        # Rich Text Format
        ".rtf": 5,
        # PDF Document
        ".pdf": 6,
        # Image
        ".jpg": 7,
        ".jpeg": 7,
        ".png": 7,
        ".gif": 7,
        ".bmp": 7,
        # Video
        ".mov": 8,
        ".mp4": 8,
        ".avi": 8,
        # Audio
        ".mp3": 9,
        ".wav": 9,
        # HTML Document
        ".html": 10,
        ".htm": 10,
        # Email
        ".msg": 11,
        ".eml": 11,
    }
    return extension_mapping.get(fileExtension.lower(), 0)  # Default to 'Ukendt' (0) if extension is not recognized


def _iter_dokument_shard_dbs(session: Session) -> list[str]:
    """
    Return the available DokumentData shard database names for the active environment.

    The environment is derived from the global ``ENV`` ("Test" or "Drift").

    :param session: SQLAlchemy Session object for database interaction
    :return: List of shard database names matching the pattern for DokumentData
    """
    return session.execute(
        text(
            f"""
            SELECT name
            FROM sys.databases
            WHERE name LIKE 'SbsysNet{ENV}Dokument[0-9][0-9][0-9][0-9]'
            ORDER BY name
            """
        )
    ).scalars().all()


def _iter_kladde_shard_dbs(session: Session) -> list[str]:
    """
    Return the available Kladde shard database names for the active environment.

    The environment is derived from the global ``ENV`` ("Test" or "Drift").

    :param session: SQLAlchemy Session object for database interaction
    :return: List of shard database names matching the pattern for Kladde
    """
    return session.execute(
        text(
            f"""
            SELECT name
            FROM sys.databases
            WHERE name LIKE 'SbsysNet{ENV}Kladde[0-9][0-9][0-9][0-9]'
            ORDER BY name
            """
        )
    ).scalars().all()


def _get_newest_shard_db(shard_dbs: list[str], shard_type_label: str) -> str:
    """
    Select the newest shard database name from a list.

    Details:
    - Uses lexicographic ``max()`` on the database name strings.
    - This assumes shard names are zero-padded and monotonically increasing (e.g. ...Dokument0001, ...Dokument0002).

    :param shard_dbs: List of shard database names
    :param shard_type_label: Label for the type of shard (e.g., "DokumentData" or "Kladde") used for logging and error messages
    :return: Name of the newest shard database
    """
    if not shard_dbs:
        raise ValueError(f"No shard databases found for {shard_type_label}.")
    return max(shard_dbs)


def _get_kladde_data_blob(session: Session, kladde_shard_dbs: list[str], kladde_id: int) -> Tuple[str, bytes] | None:
    """
    Fetch the KladdeData blob for a specific ``KladdeID`` by scanning shard databases.

    The shards are checked in the order provided; the first shard containing a matching row is returned.

    :param session: SQLAlchemy Session object for database interaction
    :param kladde_shard_dbs: List of Kladde shard database names
    :param kladde_id: ID of the Kladde for which to fetch the data blob
    :return: Tuple of (shard database name, data blob), or None if not found
    """
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
    """
    Delete a specific KladdeID from the specified Kladde shard database.

    :param session: SQLAlchemy Session object for database interaction
    :param shard_db: Name of the shard database
    :param kladde_id: ID of the Kladde to delete
    :return: Number of rows affected by the delete operation
    """
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


def _insert_dokument_data(session: Session, shard_db: str, dokument_id: int, dokument_data_info_id: int, data: bytes) -> None:
    """
    Insert a new record into ``DokumentData`` in the specified shard database.

    This function does not commit; it relies on the caller to commit/rollback the surrounding transaction.

    :param session: SQLAlchemy Session object for database interaction
    :param shard_db: Name of the shard database where the record should be inserted
    :param dokument_id: ID of the Dokument to which this data belongs
    :param dokument_data_info_id: ID of the DokumentDataInfo associated with this data
    :param data: The binary data to be inserted into the DokumentData table
    """
    session.execute(
        text(
            f"""
            INSERT INTO [{shard_db}].dbo.DokumentData (DokumentID, DokumentDataInfoID, Data)
            VALUES (:dokument_id, :dokument_data_info_id, :data)
            """
        ),
        {"dokument_id": dokument_id, "dokument_data_info_id": dokument_data_info_id, "data": data},
    )


def process_sbsys_luk(required_sagsstatus: list, required_sagsskabelon_ids: list, ignore_sagsskabelon_ids: list, dry_run: bool) -> None:
    """
    Close SBSYS cases that match a set of criteria, journalizing any drafts (kladder).

    The function performs the following steps:

    - Connects to SBSYS via ``DatabaseManager``.
    - Discovers DokumentData and Kladde shard databases and selects the newest DokumentData shard for inserts.
    - Finds cases (``Sag``) where:
        - ``Sag.SagsStatus.Navn`` is in ``required_sagsstatus``
        - ``Sag.SkabelonID`` is not in ``ignore_sagsskabelon_ids``
        - If ``required_sagsskabelon_ids`` is provided, ``Sag.SkabelonID`` is in that list
        - The case has a primary party (``Sagspart.PartType == 1``) where the linked person has civil status "Død"
    - For each matching case:
        - Marks any not-yet-completed ``Erindring`` rows as completed.
        - For each non-archived ``KladdeRegistrering`` with an attached ``Kladde`` and a corresponding ``KladdeData`` blob:
            - Creates a ``Dokument``, ``DokumentRegistrering`` and ``DokumentDataInfo`` record
            - Copies the blob from the Kladde shard into the newest DokumentData shard (``DokumentData``)
            - Creates ``DelforloebDokumentRegistrering`` rows for any distinct delforløb IDs linked to the kladde-registrering
            - Archives the ``Kladde`` and deletes the original ``KladdeData`` row from the shard
        - Updates the case status to closed (``SAG_STATUS_CLOSED``) and updates status-change fields
    - Commits all changes unless ``dry_run`` is enabled.

    :param required_sagsstatus: List of SagsStatus.Navn values that cases must have to be included
    :param required_sagsskabelon_ids: List of SkabelonID values that cases must have to be included (if empty, this filter is not applied)
    :param ignore_sagsskabelon_ids: List of SkabelonID values that cases must NOT have to be included
    :param dry_run: If True, log intended actions without making database changes
    """
    db = DatabaseManager(
        profile_name=f"sbsys_luk_{ENV}",
        db_type="mssql",
        airflow_connection_id=f"sbsys_luk_{ENV}",
    )

    with db.get_session() as session:
        dokument_shard_dbs = _iter_dokument_shard_dbs(session=session)
        kladde_shard_dbs = _iter_kladde_shard_dbs(session=session)
        newest_dokument_shard_db = _get_newest_shard_db(shard_dbs=dokument_shard_dbs, shard_type_label="DokumentData")
        logger.info(f"Newest DokumentData shard selected for inserts: {newest_dokument_shard_db}")

        # Query to fetch cases that match the criteria
        query = (
            session.query(Sag)
            .options(
                selectinload(Sag.Erindring),
                selectinload(Sag.KladdeRegistrering).selectinload(KladdeRegistrering.Kladde),
                selectinload(Sag.KladdeRegistrering).selectinload(KladdeRegistrering.DelforloebKladdeRegistrering),
            )
            .filter(
                Sag.SagsStatus.has(Sagsstatus.Navn.in_(required_sagsstatus)),
                Sag.SkabelonID.notin_(ignore_sagsskabelon_ids),
                Sag.SagsPart.has(
                    and_(
                        Sagspart.PartType == 1,
                        Sagspart.Person.has(Person.Civilstand.has(CivilstandOpslag.Navn == "Død")), # Filter for cases where Civilstand is "Død"
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
                if erindring.ErAfsluttet:
                    continue
                if dry_run:
                    logger.info(
                        f"DRY_RUN: Would complete Erindring ID {erindring.ID} for case ID {sag.ID} (set ErAfsluttet=1, Afsluttet=datetime.now(), AfsluttetAfID={USER_ID}, AfsluttetNotat='Erindring afsluttet ifm. automatisk sagslukning af robot (Digitalisering).')"
                    )
                    continue
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
                    DokumentArtID=DOKUMENT_ART_ID,
                    DokumentType=DOKUMENT_TYPE_ID,
                    OprettetAfID=USER_ID,
                    Oprettet=datetime.now(),
                    PostlisteTitel=kladde.Navn,
                    PrimaryDokumentDataInfoID=None  # Will be set after DokumentDataInfo is created
                )
                session.add(dokument)
                session.flush()

                dokument_reg = DokumentRegistrering(
                    SagID=sag.ID,
                    DokumentID=dokument.ID,
                    Navn=kladde_reg.Navn,
                    Beskrivelse=kladde_reg.Beskrivelse,
                    Registreret=datetime.now(),
                    RegistreretAfID=USER_ID
                )
                session.add(dokument_reg)
                session.flush()  # ensure dokument_reg.ID is available for FK usage below

                dokument_data_info = DokumentDataInfo(
                    DokumentID=dokument.ID,
                    DokumentDataType=_get_dokument_data_type(fileExtension=kladde.FileExtension),
                    DokumentDataInfoType=DOKUMENT_DATA_INFO_TYPE_ID,
                    FileName=kladde.FileName,
                    FileExtension=kladde.FileExtension,
                    FileSize=len(kladde_blob) if kladde_blob else 0,
                )
                session.add(dokument_data_info)
                session.flush()  # ensure dokument_data_info.ID is available for FK usage below

                delforloeb_links = kladde_reg.DelforloebKladdeRegistrering
                if delforloeb_links:
                    delforloeb_ids = [
                        getattr(link, "DelforloebID", None)
                        for link in delforloeb_links
                    ]

                    for delforloeb_id in sorted({i for i in delforloeb_ids if i is not None}):
                        dokument_delforloeb_reg = DelforloebDokumentRegistrering(
                            DokumentRegistreringID=dokument_reg.ID,
                            DelforloebID=delforloeb_id,
                        )
                        session.add(dokument_delforloeb_reg)

                dokument.PrimaryDokumentDataInfoID = dokument_data_info.ID
                session.add(dokument)
                session.flush()  # ensure dokument.PrimaryDokumentDataInfoID is set before inserting DokumentData

                _insert_dokument_data(session=session, shard_db=newest_dokument_shard_db, dokument_id=dokument.ID, dokument_data_info_id=dokument_data_info.ID, data=kladde_blob)

                kladde.IsArchived = 1
                session.add(kladde)

                deleted = _delete_kladde_data(session=session, shard_db=kladde_data_shard_db, kladde_id=kladde.ID)
                logger.info(f"Deleted {deleted} KladdeData row(s) for Kladde ID {kladde.ID} from shard {kladde_data_shard_db}")

            # Update the case status to 'Lukket'
            if dry_run:
                logger.info(
                    f"DRY_RUN: Would update case ID {sag.ID} status to 'Lukket' and set LastStatusChange fields."
                )
                continue
            sag.SagsStatusID = SAG_STATUS_CLOSED
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
