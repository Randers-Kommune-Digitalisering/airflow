

from dag_novax_district_control.novax_data import UserData
import pandas as pd
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine
from datetime import datetime

from dag_novax_district_control.novax_utils import parse_address


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


def get_sql_data(query) -> list[dict]:
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


def update_sql_data(query) -> bool:
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

    :param from_date: The start date to filter records from (inclusive).
    :param to_date: The end date to filter records to (exclusive).
    :return:
    """
    query = f"""SELECT
                    Godkommu.JOURNALDATO,
                    Godkommu.NAVNID,
                    navn.CPR,
                    navn.ADRESSE,
                    navn.DISTRIKT,
                    PERSONDISTRICT.DISTRICT AS PERSONDISTRIKT,
                    TELEFON.TELEFONNUMMER,
                    Note.NOTE
                FROM Godkommu
                LEFT JOIN navn ON Godkommu.NAVNID = navn.ID
                LEFT JOIN PERSONDISTRICT ON Godkommu.NAVNID = PERSONDISTRICT.NAVNID
                JOIN (
                    SELECT NAVNID, TELEFONNUMMER
                    FROM TELEFON
                    WHERE TS_UPDD = (
                        SELECT MAX(TS_UPDD) FROM TELEFON t2 WHERE t2.NAVNID = TELEFON.NAVNID
                    )
                ) AS TELEFON ON Godkommu.NAVNID = TELEFON.NAVNID
                LEFT JOIN Note ON Godkommu.NAVNID = Note.NAVNID AND Note.NOTE LIKE N'%gravid%'
                WHERE
                    (EMNEBREV LIKE N'%gravid%')
                AND Godkommu.JOURNALDATO >= '{from_date.strftime('%Y-%m-%d %H:%M:%S')}'
                AND Godkommu.JOURNALDATO < '{to_date.strftime('%Y-%m-%d %H:%M:%S')}'"""

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


def get_test_data(cpr=None) -> list[UserData]:
    query = """ SELECT
                    Godkommu.JOURNALDATO,
                    Godkommu.NAVNID,
                    navn.CPR,
                    navn.ADRESSE,
                    navn.DISTRIKT,
                    PERSONDISTRICT.DISTRICT AS PERSONDISTRIKT,
                    TELEFON.TELEFONNUMMER,
                    Note.NOTE
                FROM Godkommu
                LEFT JOIN navn ON Godkommu.NAVNID = navn.ID
                LEFT JOIN PERSONDISTRICT ON Godkommu.NAVNID = PERSONDISTRICT.NAVNID
                JOIN (
                    SELECT NAVNID, TELEFONNUMMER
                    FROM TELEFON
                    WHERE TS_UPDD = (
                        SELECT MAX(TS_UPDD) FROM TELEFON t2 WHERE t2.NAVNID = TELEFON.NAVNID
                    )
                ) AS TELEFON ON Godkommu.NAVNID = TELEFON.NAVNID
                LEFT JOIN Note ON Godkommu.NAVNID = Note.NAVNID AND Note.NOTE LIKE N'%gravid%'
                WHERE (EMNEBREV LIKE N'%gravid%')"""
    if cpr:
        query += f" AND navn.CPR = '{cpr}'"
    else:
        query += " LIMIT 1"

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
