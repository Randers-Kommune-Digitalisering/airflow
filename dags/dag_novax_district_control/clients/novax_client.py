from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from sqlalchemy import text
from sqlalchemy.engine import Result
from sqlalchemy.orm import Session
from datetime import date
import logging

from dag_novax_district_control.novax_utils import parse_address, to_int_or_none
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
        raise


def update_novax_userdatas_batch(updates: list[dict[str, any]]) -> dict[str, bool]:
    """Batch-update many NAVNID records using a single SQLAlchemy Session.

    This keeps 1 connection open for the whole batch and performs a single outer
    commit at the end. Each NAVNID update runs in a nested transaction (SAVEPOINT)
    so failures don't automatically abort the full batch.

    Expected update dict keys:
      - navnid: string (required)
      - due_date: date (optional)
      - new_district: string (optional)
      - new_address: Address (optional)
      - new_tlf_nr: string (optional)
      - new_municipality_code: int (optional)

    Returns: mapping {navnid: bool}
    """
    engine = _get_sqlalchemy_engine()
    results: dict[str, bool] = {}

    def _exec(session: Session, query: str, params: dict | None = None) -> Result:
        return session.execute(text(query), params or {})

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
                            new_municipality_code = upd.get("new_municipality_code")

                            if due_date is not None:
                                _exec(
                                    session,
                                    """
                                    UPDATE NAVNDETALJER
                                    SET TERMIN = :due_date,
                                        TS_UPDD = CAST(GETDATE() AS date),
                                        TS_UPDT = CONVERT(varchar(5),GETDATE(), 108)
                                    WHERE NAVNID = :navnid
                                    """,
                                    {"due_date": due_date, "navnid": navnid},
                                )

                            if new_district is not None:
                                # Update active district in navn table
                                _exec(
                                    session,
                                    """
                                    UPDATE navn
                                    SET DISTRIKT = :new_district,
                                        TS_UPDD = CAST(GETDATE() AS date),
                                        TS_UPDT = CONVERT(varchar(5),GETDATE(), 108)
                                    WHERE ID = :navnid
                                    """,
                                    {"new_district": new_district, "navnid": navnid},
                                )

                                # Close any existing district records with different district
                                _exec(
                                    session,
                                    """
                                    UPDATE PERSONDISTRICT
                                    SET DATETO = CAST(GETDATE() AS date),
                                        TS_UPDD = CAST(GETDATE() AS date),
                                        TS_UPDT = CONVERT(varchar(5),GETDATE(), 108)
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
                                        INSERT INTO PERSONDISTRICT (NAVNID, DISTRICT, DATEFROM, DATETO, TS_DATE, TS_TIME, TS_UPDD, TS_UPDT)
                                        VALUES (:navnid,
                                                :new_district,
                                                CAST(GETDATE() AS date),
                                                '1753-01-01 00:00:00.000',
                                                CAST(GETDATE() AS date),
                                                CONVERT(varchar(5),GETDATE(), 108),
                                                CAST(GETDATE() AS date),
                                                CONVERT(varchar(5),GETDATE(), 108)
                                            )
                                    END
                                    """,
                                    {"new_district": new_district, "navnid": navnid},
                                )

                            if new_address is not None:
                                # Update active address in navn table
                                _exec(
                                    session,
                                    """
                                    UPDATE navn
                                    SET ADRESSE = :new_address,
                                        TS_UPDD = CAST(GETDATE() AS date),
                                        TS_UPDT = CONVERT(varchar(5),GETDATE(), 108)
                                    WHERE ID = :navnid
                                    """,
                                    {"new_address": new_address.full_address, "navnid": navnid},
                                )

                                # Close any existing address records in adrs table
                                _exec(
                                    session,
                                    """
                                    UPDATE adrs
                                    SET DATO_TIL = CAST(GETDATE() AS date),
                                        TS_UPDD = CAST(GETDATE() AS date),
                                        TS_UPDT = CONVERT(varchar(5),GETDATE(), 108)
                                    WHERE NAVNID = :navnid
                                      AND DATO_FRA <= CAST(GETDATE() AS date)
                                      AND (DATO_TIL > CAST(GETDATE() AS date) OR DATO_TIL = '1753-01-01 00:00:00.000')
                                    """,
                                    {"navnid": navnid},
                                )

                                # Insert new address record if not already present
                                from dag_novax_district_control.check_and_update_district import DEFAULT_MUNICIPALITY_CODE
                                _exec(
                                    session,
                                    """
                                    IF NOT EXISTS (
                                        SELECT 1
                                        FROM adrs
                                        WHERE NAVNID = :navnid
                                          AND VEJKODE = :vejkode
                                          AND POSTNR = :postnr
                                          AND NR_LT_ETAGE = :nr_lt_etage
                                          AND DATO_FRA <= CAST(GETDATE() AS date)
                                          AND (DATO_TIL IS NULL OR DATO_TIL >= CAST(GETDATE() AS date) OR DATO_TIL = '1753-01-01 00:00:00.000')
                                    )
                                    BEGIN
                                        INSERT INTO adrs (NAVNID, VEJKODE, POSTNR, NR_LT_ETAGE, KOMMUNEKODE, DATO_FRA, DATO_TIL, TS_DATE, TS_TIME, TS_UPDD, TS_UPDT)
                                        VALUES (:navnid,
                                                :vejkode,
                                                :postnr,
                                                :nr_lt_etage,
                                                :kommunekode,
                                                CAST(GETDATE() AS date),
                                                '1753-01-01 00:00:00.000',
                                                CAST(GETDATE() AS date),
                                                CONVERT(varchar(5),GETDATE(), 108),
                                                CAST(GETDATE() AS date),
                                                CONVERT(varchar(5),GETDATE(), 108)
                                            )
                                    END
                                    """,
                                    {
                                        "navnid": navnid,
                                        "vejkode": new_address.street_code,
                                        "postnr": new_address.postal_code,
                                        "nr_lt_etage": (
                                            str((new_address.number or "") + " " + (new_address.door_extension or "")).strip()
                                        ),
                                        "kommunekode": new_municipality_code or DEFAULT_MUNICIPALITY_CODE,
                                    },
                                )

                                # Set BESKYTTETADRESSE to match CPR protected address flag
                                _exec(
                                    session,
                                    """
                                    UPDATE NAVNDETALJER
                                    SET BESKYTTETADRESSE = :protected_address,
                                        TS_UPDD = CAST(GETDATE() AS date),
                                        TS_UPDT = CONVERT(varchar(5),GETDATE(), 108)
                                    WHERE NAVNID = :navnid
                                    """,
                                    {"navnid": navnid, "protected_address": 1 if new_address.is_protected else 0},
                                )

                            if new_tlf_nr is not None:
                                # Make the provided number primary and demote any existing primary number
                                _exec(
                                    session,
                                    """
                                    UPDATE TELEFON
                                    SET PRIMAER = 0,
                                        TS_UPDD = GETDATE(),
                                        TS_UPDT = CONVERT(varchar(5),GETDATE(), 108)
                                    WHERE NAVNID = :navnid
                                      AND TELEFONNUMMER <> :new_tlf_nr
                                      AND PRIMAER = 1
                                    """,
                                    {"navnid": navnid, "new_tlf_nr": new_tlf_nr},
                                )

                                res = _exec(
                                    session,
                                    """
                                    UPDATE TELEFON
                                    SET PRIMAER = 1,
                                        TS_UPDD = GETDATE(),
                                        TS_UPDT = CONVERT(varchar(5),GETDATE(), 108)
                                    WHERE NAVNID = :navnid
                                      AND TELEFONNUMMER = :new_tlf_nr
                                    """,
                                    {"navnid": navnid, "new_tlf_nr": new_tlf_nr},
                                )

                                updated_rows = res.rowcount if res is not None else 0
                                if updated_rows == 0:
                                    _exec(
                                        session,
                                        """
                                        INSERT INTO TELEFON (NAVNID, TELEFONNUMMER, PRIMAER, TS_DATE, TS_TIME, TS_UPDD, TS_UPDT)
                                        VALUES (:navnid,
                                                :new_tlf_nr,
                                                1,
                                                CAST(GETDATE() AS date),
                                                CONVERT(varchar(5),GETDATE(), 108),
                                                CAST(GETDATE() AS date),
                                                CONVERT(varchar(5),GETDATE(), 108)
                                            )
                                        """,
                                        {"navnid": navnid, "new_tlf_nr": new_tlf_nr},
                                    )

                            # Update municipality code
                            if new_municipality_code is not None:
                                _exec(
                                    session,
                                    """
                                    UPDATE NAVNDETALJER
                                    SET TS_KOMID = :new_municipality_code,
                                        KOMMUNE_OPR = :new_municipality_code,
                                        TS_UPDD = CAST(GETDATE() AS date),
                                        TS_UPDT = CONVERT(varchar(5),GETDATE(), 108)
                                    WHERE NAVNID = :navnid
                                    """,
                                    {"navnid": navnid, "new_municipality_code": new_municipality_code},
                                )

                            # Always allocate new pregnancy to 'Gravid til fordeling' (id: 'FIKTIV')
                            _exec(
                                session,
                                """
                                UPDATE navn
                                SET AnsvarsShpl = 'FIKTIV',
                                    TS_UPDD = CAST(GETDATE() AS date),
                                    TS_UPDT = CONVERT(varchar(5),GETDATE(), 108)
                                WHERE ID = :navnid
                                """,
                                {"navnid": navnid},
                            )

                            # Always set new pregnancy to active
                            _exec(
                                session,
                                """
                                UPDATE navn
                                SET AKTIV = 1,
                                    TS_UPDD = CAST(GETDATE() AS date),
                                    TS_UPDT = CONVERT(varchar(5),GETDATE(), 108)
                                WHERE ID = :navnid
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


def get_pregnancy_journals(from_date: date, to_date: date) -> list[UserData]:
    """
    Retrieves pregnancy journal records from Novax database within the specified date range.
    Ensures only the latest TELEFONNUMMER and relevant NOTE per CPR is returned.

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
            NAVNDETALJER.KOMMUNE_OPR,
            NAVNDETALJER.BESKYTTETADRESSE,
            NAVNDETALJER.TERMIN,
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
        LEFT JOIN
            NAVNDETALJER ON Godkommu.NAVNID = NAVNDETALJER.NAVNID
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

        parsed_address = parse_address(entry.get('ADRESSE') or "")
        if parsed_address is not None:
            parsed_address.is_protected = entry.get('BESKYTTETADRESSE') == 1
        entry['parsed_address'] = parsed_address
        entry['timestamp'] = entry['JOURNALDATO'].strftime('%Y-%m-%d %H:%M:%S') if entry.get('JOURNALDATO') else None

        data_obj = UserData(
            cpr=entry['CPR'],
            navnid=entry['NAVNID'],
            address=entry['parsed_address'],
            district=entry['DISTRIKT'],
            municipality_code=to_int_or_none(entry.get('KOMMUNE_OPR')),
            tlf_nr=entry['TELEFONNUMMER'],
            due_date=entry['TERMIN'],
            timestamp=entry['JOURNALDATO'],
            journal=entry['NOTE']
        )

        if data_obj.journal is None:
            logger.error(f"Missing NOTE for NAVNID {data_obj.navnid} with CPR {data_obj.cpr} at {data_obj.timestamp}")
            continue

        userdata_list.append(data_obj)
    return userdata_list
