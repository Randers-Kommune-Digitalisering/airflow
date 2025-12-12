

from dag_novax_district_control.novax_data import UserData
import pandas as pd
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine
from datetime import datetime
import logging

from dag_novax_district_control.novax_utils import parse_address

logger = logging.getLogger(__name__)


def get_sqlalchemy_engine():
    airflow_conn = BaseHook.get_connection("novax_sql_default")
    user = airflow_conn.login
    password = airflow_conn.password
    host = airflow_conn.host
    db = airflow_conn.schema
    connection_string = f"mssql+pymssql://{user}:{password}@{host}/{db}"
    engine = create_engine(connection_string)
    return engine


def test_connection() -> bool:
    import logging
    logger = logging.getLogger(__name__)
    airflow_conn = BaseHook.get_connection("novax_sql_default")
    logger.info(f"Trying to connect with Airflow connection: id={airflow_conn.conn_id}, host={airflow_conn.host}, schema={airflow_conn.schema}, login={airflow_conn.login}, port={airflow_conn.port}, extra={airflow_conn.extra}")
    try:
        engine = get_sqlalchemy_engine()
        with engine.connect() as conn:
            logger.info(f'Connection to database {airflow_conn.schema} successful: {conn}')
            return True
    except Exception as e:
        logger.error(f'Failed to connect to database: {e}')
        return False


def get_sql_data(query: str) -> list[dict]:
    engine = get_sqlalchemy_engine()
    conn = None
    try:
        conn = engine.connect()
        result = pd.read_sql(query, con=conn)
        records = result.to_dict(orient='records')
        if isinstance(records, dict):
            return [records]
        elif isinstance(records, list):
            return records
        else:
            return []
    except Exception as e:
        print(f'Error executing query: {e}')
        return []
    finally:
        if conn:
            conn.close()


def update_sql_data(query: str) -> bool:
    engine = get_sqlalchemy_engine()
    conn = None
    try:
        conn = engine.connect()
        trans = conn.begin()
        conn.execute(query)
        trans.commit()
        return True
    except Exception:
        if conn:
            try:
                trans.rollback()
            except Exception:
                pass
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
    query = f"""
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
            AND Godkommu.JOURNALDATO >= '{from_date.strftime('%Y-%m-%d %H:%M:%S')}'
            AND Godkommu.JOURNALDATO < '{to_date.strftime('%Y-%m-%d %H:%M:%S')}'
        GROUP BY
            Godkommu.JOURNALDATO,
            Godkommu.NAVNID,
            navn.CPR,
            navn.ADRESSE,
            navn.DISTRIKT
    """

    # If PERSONDISTRICT.DISTRICT is needed, use a subquery to get only the latest/current row per NAVNID.
    # Example:
    # LEFT JOIN (
    #     SELECT NAVNID, DISTRICT
    #     FROM (
    #         SELECT *, ROW_NUMBER() OVER (PARTITION BY NAVNID ORDER BY DATEFROM DESC) AS rn
    #         FROM PERSONDISTRICT
    #     ) pd WHERE rn = 1
    # ) pd ON Godkommu.NAVNID = pd.NAVNID

    data = get_sql_data(query)
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

    success = []

    # Update due date if provided
    if due_date is not None:
        due_date_str = due_date.strftime('%Y-%m-%d %H:%M:%S')
        query = f"""
            UPDATE NAVNDETALJER
            SET TERMIN = CAST('{due_date_str}' AS DATETIME)
            WHERE ID = {navnid}
        """
        res = update_sql_data(query)
        success.append(res)
        logger.info(f"Updated NAVNDETALJER.TERMIN for NAVNID {navnid} to {due_date_str} {'was successful' if res else 'failed'}.")

    # Update district if provided
    if new_district is not None:
        query1 = f"""
            UPDATE navn
            SET DISTRIKT = N'{new_district}'
            WHERE ID = {navnid}
        """
        res1 = update_sql_data(query1)
        success.append(res1)
        logger.info(f"Updated navn.DISTRIKT for NAVNID {navnid} to {new_district} {'was successful' if res1 else 'failed'}.")

        query2 = f"""
            UPDATE PERSONDISTRICT
            SET DISTRICT = N'{new_district}'
            WHERE NAVNID = {navnid}
            AND DATEFROM <= GETDATE()
            AND (DATETO IS NULL OR DATETO >= GETDATE() OR DATETO = '1753-01-01 00:00:00.000')
        """
        res2 = update_sql_data(query2)
        success.append(res2)
        logger.info(f"Updated PERSONDISTRICT.DISTRICT for NAVNID {navnid} to {new_district} {'was successful' if res2 else 'failed'}.")

    # Update address if provided
    if new_address is not None:
        query = f"""
            UPDATE navn
            SET ADRESSE = N'{new_address}'
            WHERE ID = {navnid}
        """
        res = update_sql_data(query)
        success.append(res)
        logger.info(f"Updated navn.ADRESSE for NAVNID {navnid} to {new_address} {'was successful' if res else 'failed'}.")

    # Update telephone number if provided
    if new_tlf_nr is not None:
        query = f"""
            UPDATE TELEFON
            SET TELEFONNUMMER = N'{new_tlf_nr}'
            WHERE NAVNID = {navnid}
            AND TS_UPDD = (
                SELECT MAX(TS_UPDD)
                FROM TELEFON
                WHERE NAVNID = {navnid}
            )
        """
        res = update_sql_data(query)
        success.append(res)
        logger.info(f"Updated TELEFON.TELEFONNUMMER for NAVNID {navnid} to {new_tlf_nr} {'was successful' if res else 'failed'}.")

    # Also update area code to 730
    query = f"""
        UPDATE NAVNDETALJER
        SET TS_KOMID = 730,
            KOMMUNE_OPR = 730
        WHERE ID = {navnid}
    """
    res = update_sql_data(query)
    success.append(res)
    logger.info(f"Updated NAVNDETALJER.TS_KOMID and NAVNDETALJER.KOMMUNE_OPR for NAVNID {navnid} to 730 {'was successful' if res else 'failed'}.")

    return success


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
