from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from sqlalchemy import text
from sqlalchemy.orm import Session
from datetime import datetime
import logging

from dag_novax_district_control.novax_utils import parse_address
from dag_novax_district_control.novax_utils import UserData

logger = logging.getLogger(__name__)
engine = None


def _get_sqlalchemy_engine():
    """
    Create and return a SQLAlchemy engine using Airflow connection settings.
    """
    global engine
    if engine is not None:
        return engine
    hook = MsSqlHook(mssql_conn_id="novax_sql")
    engine = hook.get_sqlalchemy_engine()
    return engine


def _get_sql_data(query: str, params: dict | None = None) -> list[dict]:
    """
    Execute a SQL query and return the results as a list of dictionaries.

    :param query: The SQL query to execute.
    :param params: Optional dictionary of parameters to bind to the query.
    """
    engine = _get_sqlalchemy_engine()
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query), params or {})
            return [dict(row) for row in result.mappings().all()]
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        return []


def update_novax_userdatas_batch(updates: list[dict]) -> dict:
    """Batch-update many NAVNID records using a single SQLAlchemy Session.

    This keeps 1 connection open for the whole batch and performs a single outer
    commit at the end. Each NAVNID update runs in a nested transaction (SAVEPOINT)
    so failures don't automatically abort the full batch.

    Expected update dict keys:
      - navnid (required)
      - due_date (optional)
      - new_district (optional)
      - new_address (optional)
      - new_tlf_nr (optional)

    Returns: mapping {navnid: bool}
    """
    engine = _get_sqlalchemy_engine()
    results: dict = {}

    def _exec(session: Session, query: str, params: dict | None = None) -> None:
        session.execute(text(query), params or {})

    def _exec_with_rowcount(
        session: Session, query: str, params: dict | None = None
    ) -> int:
        result = session.execute(text(query), params or {})
        return int(getattr(result, "rowcount", 0) or 0)

    with Session(engine) as session:
        try:
            with session.begin():
                for upd in updates:
                    navnid = upd.get("navnid")
                    if navnid is None:
                        logger.error("Batch update entry missing required 'navnid': %r, skipping entry", upd)
                        continue

                    try:
                        with session.begin_nested():
                            due_date = upd.get("due_date")
                            new_district = upd.get("new_district")
                            new_address = upd.get("new_address")
                            new_tlf_nr = upd.get("new_tlf_nr")

                            if due_date is not None:
                                _exec(
                                    session,
                                    """
                                    UPDATE NAVNDETALJER
                                    SET TERMIN = :due_date
                                    WHERE NAVNID = :navnid
                                    """,
                                    {"due_date": due_date, "navnid": navnid},
                                )

                            if new_district is not None:
                                _exec(
                                    session,
                                    """
                                    UPDATE navn
                                    SET DISTRIKT = :new_district
                                    WHERE ID = :navnid
                                    """,
                                    {"new_district": new_district, "navnid": navnid},
                                )

                                # Close any existing district records with different district
                                _exec(
                                    session,
                                    """
                                    UPDATE PERSONDISTRICT
                                    SET DATETO = CAST(GETDATE() AS date)
                                    WHERE NAVNID = :navnid
                                      AND DATEFROM <= CAST(GETDATE() AS date)
                                      AND (DATETO > CAST(GETDATE() AS date) OR DATETO = '1753-01-01 00:00:00.000')
                                      AND DISTRICT <> :new_district
                                    """,
                                    {"new_district": new_district, "navnid": navnid},
                                )

                                # Insert new district record if not already present
                                _exec(
                                    session,
                                    """
                                    IF NOT EXISTS (
                                        SELECT 1
                                        FROM PERSONDISTRICT
                                        WHERE NAVNID = :navnid
                                          AND DISTRICT = :new_district
                                          AND DATEFROM <= CAST(GETDATE() AS date)
                                          AND (DATETO IS NULL OR DATETO >= CAST(GETDATE() AS date) OR DATETO = '1753-01-01 00:00:00.000')
                                    )
                                    BEGIN
                                        INSERT INTO PERSONDISTRICT (NAVNID, DISTRICT, DATEFROM, DATETO)
                                        VALUES (:navnid, :new_district, CAST(GETDATE() AS date), '1753-01-01 00:00:00.000')
                                    END
                                    """,
                                    {"new_district": new_district, "navnid": navnid},
                                )

                            if new_address is not None:
                                _exec(
                                    session,
                                    """
                                    UPDATE navn
                                    SET ADRESSE = :new_address
                                    WHERE ID = :navnid
                                    """,
                                    {"new_address": new_address, "navnid": navnid},
                                )

                            if new_tlf_nr is not None:
                                # Make the provided number primary and demote all others
                                _exec(
                                    session,
                                    """
                                    UPDATE TELEFON
                                    SET PRIMAER = 0
                                    WHERE NAVNID = :navnid
                                      AND TELEFONNUMMER <> :new_tlf_nr
                                    """,
                                    {"navnid": navnid, "new_tlf_nr": new_tlf_nr},
                                )

                                updated_rows = _exec_with_rowcount(
                                    session,
                                    """
                                    UPDATE TELEFON
                                    SET PRIMAER = 1,
                                        TS_UPDD = GETDATE()
                                    WHERE NAVNID = :navnid
                                      AND TELEFONNUMMER = :new_tlf_nr
                                    """,
                                    {"navnid": navnid, "new_tlf_nr": new_tlf_nr},
                                )

                                if updated_rows == 0:
                                    _exec(
                                        session,
                                        """
                                        INSERT INTO TELEFON (NAVNID, TELEFONNUMMER, PRIMAER, TS_UPDD)
                                        VALUES (:navnid, :new_tlf_nr, 1, GETDATE())
                                        """,
                                        {"navnid": navnid, "new_tlf_nr": new_tlf_nr},
                                    )

                            # Always update area code to 730
                            _exec(
                                session,
                                """
                                UPDATE NAVNDETALJER
                                SET TS_KOMID = 730,
                                    KOMMUNE_OPR = 730
                                WHERE NAVNID = :navnid
                                """,
                                {"navnid": navnid},
                            )

                        results[navnid] = True
                    except Exception as e:
                        logger.error(f"Batch update failed for NAVNID {navnid!r}: {e}")
                        results[navnid] = False

        except Exception as e:
            logger.error(f"Batch commit failed; rolling back whole batch: {e}")
            try:
                session.rollback()
            except Exception:
                pass
            results = {upd.get("navnid"): False for upd in updates}

    return results


def get_pregnancy_journals(from_date: datetime.date, to_date: datetime.date) -> list[UserData]:
    """
    Retrieves pregnancy journal records from Novax database within the specified date range.

    Ensures only the latest TELEFONNUMMER per CPR is returned.

    :param from_date: The start date to filter records from (inclusive).
    :param to_date: The end date to filter records to (exclusive).
    """
    query = """
        SELECT
            Godkommu.JOURNALDATO,
            Godkommu.NAVNID,
            navn.CPR,
            navn.ADRESSE,
            navn.DISTRIKT,
            (
                SELECT TOP 1 TELEFONNUMMER
                FROM TELEFON
                WHERE TELEFON.NAVNID = Godkommu.NAVNID
                ORDER BY TS_UPDD DESC
            ) AS TELEFONNUMMER,
            (
                SELECT TOP 1 NOTE
                FROM Note
                WHERE Note.NAVNID = Godkommu.NAVNID
                AND Note.NOTE LIKE N'%Orientering - Gravid%'
                AND CAST(Note.DATO AS DATE) = CAST(Godkommu.JOURNALDATO AS DATE)
                ORDER BY TS_DATE DESC
            ) AS NOTE
        FROM
            Godkommu
        LEFT JOIN
            navn ON Godkommu.NAVNID = navn.ID
        WHERE
            (EMNEBREV LIKE N'%Orientering - Gravid%')
            AND Godkommu.JOURNALDATO >= :from_date
            AND Godkommu.JOURNALDATO < :to_date
    """

    data = _get_sql_data(query, params={"from_date": from_date, "to_date": to_date})
    if not data:
        return []

    userdata_list = []
    for entry in data:
        for k, v in entry.items():
            if isinstance(v, str):
                entry[k] = v.strip()
        entry['parsed_address'] = parse_address(entry['ADRESSE'])
        entry['timestamp'] = entry['JOURNALDATO'].strftime('%Y-%m-%d %H:%M:%S') if entry.get('JOURNALDATO') else None

        data_obj = UserData(
            cpr=entry['CPR'],
            navnid=entry['NAVNID'],
            address=entry['parsed_address'],
            district=entry['DISTRIKT'],
            tlf_nr=entry['TELEFONNUMMER'],
            timestamp=entry['JOURNALDATO'],
            journal=entry['NOTE']
        )
        userdata_list.append(data_obj)
    return userdata_list
