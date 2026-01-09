from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import pandas as pd
from airflow.hooks.base import BaseHook
from sqlalchemy import text
from datetime import datetime
import logging

from dag_novax_district_control.novax_utils import parse_address
from dag_novax_district_control.novax_data import UserData

logger = logging.getLogger(__name__)


def get_sqlalchemy_engine():
    """
    Create and return a SQLAlchemy engine using Airflow connection settings.
    """
    hook = MsSqlHook(mssql_conn_id="novax_sql")
    engine = hook.get_sqlalchemy_engine()
    return engine


def test_connection() -> bool:
    """
    Test the connection to the Novax database using Airflow connection settings.
    """
    airflow_conn = BaseHook.get_connection("novax_sql")
    logger.info(f"Trying to connect with Airflow connection: id={airflow_conn.conn_id}, host={airflow_conn.host}, schema={airflow_conn.schema}, login={airflow_conn.login}, port={airflow_conn.port}, extra={airflow_conn.extra}")
    try:
        engine = get_sqlalchemy_engine()
        with engine.connect() as conn:
            logger.info(f'Connection to database {airflow_conn.schema} successful: {conn}')
            return True
    except Exception as e:
        logger.error(f'Failed to connect to database: {e}')
        return False


def get_sql_data(query: str, params: dict | None = None) -> list[dict]:
    """
    Execute a SQL query and return the results as a list of dictionaries.

    :param query: The SQL query to execute.
    :param params: Optional dictionary of parameters to bind to the query.
    """
    engine = get_sqlalchemy_engine()
    conn = None
    try:
        conn = engine.connect()
        # Use SQLAlchemy `text()` + bound parameters to avoid SQL injection.
        result = pd.read_sql_query(text(query), con=conn, params=params)
        records = result.to_dict(orient='records')
        if isinstance(records, dict):
            return [records]
        elif isinstance(records, list):
            return records
        else:
            return []
    except Exception as e:
        logger.error(f'Error executing query: {e}')
        return []
    finally:
        if conn:
            conn.close()


def update_sql_data(query: str, params: dict | None = None) -> bool:
    """
    Execute an update/insert/delete SQL command.

    :param query: The SQL query to execute.
    :param params: Optional dictionary of parameters to bind to the query.
    """
    engine = get_sqlalchemy_engine()
    conn = None
    try:
        conn = engine.connect()
        trans = conn.begin()
        conn.execute(text(query), params or {})
        trans.commit()
        return True
    except Exception as e:
        logger.error(f"SQL error executing update: {e}")
        if conn:
            try:
                trans.rollback()
            except Exception as rollback_err:
                logger.error(f"Error during transaction rollback: {rollback_err}")
        return False
    finally:
        if conn:
            conn.close()


def get_pregnancy_journals(from_date: datetime, to_date: datetime) -> list[UserData]:
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
            (EMNEBREV LIKE N'%gravid%')
            AND Godkommu.JOURNALDATO >= :from_date
            AND Godkommu.JOURNALDATO < :to_date
        GROUP BY
            Godkommu.JOURNALDATO,
            Godkommu.NAVNID,
            navn.CPR,
            navn.ADRESSE,
            navn.DISTRIKT
    """

    data = get_sql_data(query, params={"from_date": from_date, "to_date": to_date})
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


def get_upcoming_due_dates(from_date: datetime) -> list[UserData]:
    """
    Retrieves pregnancy records with due dates within the specified date range.

    :param from_date: The start date to filter due dates from (inclusive).
    """
    query = """
        SELECT
            NAVNDETALJER.NAVNID,
            navn.CPR,
            navn.ADRESSE,
            navn.DISTRIKT,
            NAVNDETALJER.TERMIN
        FROM
            NAVNDETALJER
        LEFT JOIN
            navn ON NAVNDETALJER.NAVNID = navn.ID
        WHERE
            NAVNDETALJER.TERMIN >= :from_date
    """

    data = get_sql_data(query, params={"from_date": from_date})
    if not data:
        return []

    userdata_list = []
    for entry in data:
        for k, v in entry.items():
            if isinstance(v, str):
                entry[k] = v.strip()
        entry['parsed_address'] = parse_address(entry['ADRESSE'])

        # due_date_value = entry.get('TERMIN')
        # if isinstance(due_date_value, pd.Timestamp):
        #     due_date_value = due_date_value.to_pydatetime()

        data_obj = UserData(
            cpr=entry['CPR'],
            navnid=entry['NAVNID'],
            address=entry['parsed_address'],
            district=entry['DISTRIKT'],
            tlf_nr=None,
            timestamp=None,
            journal=None
        )
        userdata_list.append(data_obj)
    return userdata_list


def update_novax_userdata(navnid: int, due_date: datetime = None, new_district: str = None, new_address: str = None, new_tlf_nr: str = None) -> bool:
    """
    Updates the DISTRIKT field for a given NAVNID in the navn table.

    :param navnid: The NAVNID of the record to update (required).
    :param due_date: The new due date to set (optional).
    :param new_district: The new district value to set (optional).
    :param new_address: The new address to set (optional).
    :param new_tlf_nr: The new telephone number to set (optional).
    """
    if due_date is None and new_district is None and new_address is None and new_tlf_nr is None:
        return True  # Nothing to update

    if navnid is None:
        logger.error("NAVNID is required to update Novax userdata.")
        return False

    # Ensure NAVNID is an integer (guards against accidental unsafe string input).
    try:
        navnid = int(navnid)
    except (TypeError, ValueError):
        logger.error(f"Invalid NAVNID value: {navnid!r}")
        return False

    success = []

    # Update due date if provided
    if due_date is not None:
        query = """
            UPDATE NAVNDETALJER
            SET TERMIN = :due_date
            WHERE NAVNID = :navnid
        """
        res = update_sql_data(query, params={"due_date": due_date, "navnid": navnid})
        success.append(res)
        logger.info(f"Updated NAVNDETALJER.TERMIN for NAVNID {navnid} to {due_date} {'was successful' if res else 'failed'}.")

    # Update district if provided
    if new_district is not None:
        query1 = """
            UPDATE navn
            SET DISTRIKT = :new_district
            WHERE ID = :navnid
        """
        res1 = update_sql_data(query1, params={"new_district": new_district, "navnid": navnid})
        success.append(res1)
        logger.info(f"Updated navn.DISTRIKT for NAVNID {navnid} to {new_district} {'was successful' if res1 else 'failed'}.")

        query2 = """
            UPDATE PERSONDISTRICT
            SET DISTRICT = :new_district
            WHERE NAVNID = :navnid
            AND DATEFROM <= GETDATE()
            AND (DATETO IS NULL OR DATETO >= GETDATE() OR DATETO = '1753-01-01 00:00:00.000')
        """
        res2 = update_sql_data(query2, params={"new_district": new_district, "navnid": navnid})
        success.append(res2)
        logger.info(f"Updated PERSONDISTRICT.DISTRICT for NAVNID {navnid} to {new_district} {'was successful' if res2 else 'failed'}.")

    # Update address if provided
    if new_address is not None:
        query = """
            UPDATE navn
            SET ADRESSE = :new_address
            WHERE ID = :navnid
        """
        res = update_sql_data(query, params={"new_address": new_address, "navnid": navnid})
        success.append(res)
        logger.info(f"Updated navn.ADRESSE for NAVNID {navnid} to {new_address} {'was successful' if res else 'failed'}.")

    # Update telephone number if provided
    if new_tlf_nr is not None:
        # First, delete all existing TELEFON records for the NAVNID
        delete_query = """
            DELETE FROM TELEFON
            WHERE NAVNID = :navnid
        """
        delete_res = update_sql_data(delete_query, params={"navnid": navnid})
        logger.info(f"Deleted existing TELEFON records for NAVNID {navnid} {'was successful' if delete_res else 'failed'}.")

        # Then, insert the new telephone number
        query = """
            INSERT INTO TELEFON (NAVNID, TELEFONNUMMER, PRIMAER, TS_UPDD)
            VALUES (:navnid, :new_tlf_nr, 1, GETDATE())
        """
        res = update_sql_data(query, params={"navnid": navnid, "new_tlf_nr": new_tlf_nr})
        success.append(res)
        logger.info(f"Updated TELEFON.TELEFONNUMMER for NAVNID {navnid} to {new_tlf_nr} {'was successful' if res else 'failed'}.")

    # Also update area code to 730
    query = """
        UPDATE NAVNDETALJER
        SET TS_KOMID = 730,
            KOMMUNE_OPR = 730
        WHERE NAVNID = :navnid
    """
    res = update_sql_data(query, params={"navnid": navnid})
    success.append(res)
    logger.info(f"Updated NAVNDETALJER.TS_KOMID and NAVNDETALJER.KOMMUNE_OPR for NAVNID {navnid} to 730 {'was successful' if res else 'failed'}.")

    return all(success) if success else True


def get_test_data_move() -> list[dict]:
    query = """SELECT TOP 10 FLYTTE.NAVNID, navn.ADRESSE, navn.DISTRIKT, PERSONDISTRICT.DISTRICT AS PERSONDISTRIKT
               FROM FLYTTE
               LEFT JOIN navn ON FLYTTE.NAVNID = navn.ID
               LEFT JOIN PERSONDISTRICT ON FLYTTE.NAVNID = PERSONDISTRICT.NAVNID"""

    data = get_sql_data(query)
    for entry in data:
        for k, v in entry.items():
            if isinstance(v, str):
                entry[k] = v.strip()
        entry['parsed_address'] = parse_address(entry['ADRESSE'])
    return data
