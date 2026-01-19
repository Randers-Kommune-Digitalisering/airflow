import logging
import io
import pandas as pd

from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from datetime import datetime
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.sftp.hooks.sftp import SFTPHook
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse
from dag_asset.model import Base, Department, User, Computer
from airflow.models import Variable


logger = logging.getLogger(__name__)


def create_asset_tables(db_engine: Engine) -> bool:
    """
    Create asset database tables if they do not exist.

    :param db_engine: SQLAlchemy Engine for the Asset DB.
    :return: True if table creation succeeded, otherwise False.
    """
    try:
        Base.metadata.create_all(db_engine)
        logger.info("Created Computer, Bruger, and Afdeling tables if not exists.")
        return True
    except Exception as e:
        logger.error(f"Error creating tables: {e}")
        return False


def insert_departments_data(capa_cms: Engine, asset_engine: Engine) -> bool:
    """
    Fetch departments from CAPA DB and store them in asset DB.

    :param capa_cms: SQLAlchemy Engine for the CAPA CMS DB.
    :param asset_engine: SQLAlchemy Engine for the Asset DB.
    :return: True if departments were inserted/updated successfully, otherwise False.
    """
    sql_command = """
        SELECT DISTINCT USI.VALUE AS DEPARTMENT
        FROM USI
        WHERE USI.SECTION = 'General User Inventory'
          AND USI.NAME = 'Department'
    """

    logger.info(f"Executing Department SQL command: {sql_command}")

    try:
        with capa_cms.connect() as conn:
            result = conn.execute(sql_command).fetchall()
            logger.debug(f"Department SQL result: {result}")

        if not result:
            logger.error("No department data found")
            return False

        with Session(asset_engine) as session:
            existing_departments = {
                d.name for d in session.query(Department.name).all()
            }

            inserted = 0
            for (department,) in result:
                department = department.strip().lower() if isinstance(department, str) else department
                if department not in existing_departments:
                    session.add(Department(name=department))
                    inserted += 1

            session.commit()
            logger.info(f"Inserted {inserted} unique departments into Department table.")

        return True

    except Exception as e:
        logger.error(f"Error inserting departments into Afdeling table: {e}")
        return False


def insert_users_data(capa_cms: Engine, asset_engine: Engine) -> bool:
    """
    Fetch users and department relations from CAPA DB and store them in Asset DB.

    :param capa_cms: SQLAlchemy Engine for the CAPA CMS DB.
    :param asset_engine: SQLAlchemy Engine for the Asset DB.
    :return: True if users were inserted/updated successfully, otherwise False.
    """
    sql_command = """
        WITH PrimaryUsers AS (
            SELECT DISTINCT UNIT.UNITID,
                   REPLACE(REPLACE(LGI.VALUE, '@LAKSEN04', ''), '@RANDERS.DK', '') AS PRIMARY_USER
            FROM UNIT
            JOIN LGI ON UNIT.UNITID = LGI.UNITID
            WHERE LGI.SECTION = 'Current Logon'
              AND LGI.NAME = 'User Name'
        )
        SELECT DISTINCT
            DU.PRIMARY_USER,
            USI.VALUE AS FULLNAME,
            LOWER(USI2.VALUE) AS DEPARTMENT
        FROM PrimaryUsers DU
        JOIN UNIT ON UNIT.NAME = DU.PRIMARY_USER
        JOIN USI ON UNIT.UNITID = USI.UNITID
            AND USI.SECTION = 'General User Inventory'
            AND USI.NAME = 'Full Name'
        JOIN USI USI2 ON UNIT.UNITID = USI2.UNITID
            AND USI2.SECTION = 'General User Inventory'
            AND USI2.NAME = 'Department'
    """

    logger.info(f"Executing User SQL command: {sql_command}")

    try:
        with capa_cms.connect() as conn:
            result = conn.execute(sql_command).fetchall()
            logger.debug(f"User SQL result: {result}")

        if not result:
            logger.error("No user data found")
            return False

        with Session(asset_engine) as session:
            departments = {d.name: d for d in session.query(Department).all()}
            users = {u.primary_user: u for u in session.query(User).all()}

            inserted = 0

            for primary_user, fullname, department in result:
                department = department.lower() if isinstance(department, str) else department

                user = users.get(primary_user)
                if not user:
                    user = User(
                        primary_user=primary_user,
                        full_name=fullname
                    )
                    session.add(user)
                    session.flush()
                    users[primary_user] = user
                    inserted += 1

                dept_obj = departments.get(department)
                if dept_obj and dept_obj not in user.departments:
                    user.departments.append(dept_obj)

            session.commit()
            logger.info(f"Inserted/updated {inserted} users and linked departments.")

        return True

    except Exception as e:
        logger.error(f"Error inserting users into User table: {e}")
        return False


def insert_computers_data(capa_cms: Engine, asset_engine: Engine) -> bool:
    """
    Fetch computer data from CAPA DB and store/update in Asset DB.

    :param capa_cms: SQLAlchemy Engine for the CAPA CMS DB.
    :param asset_engine: SQLAlchemy Engine for the Asset DB.
    :return: True if computers were inserted/updated successfully, otherwise False.
    """
    sql_command = """
        SELECT
            U.NAME AS UnitName,
            INV.VALUE AS Producent,
            CSI.VALUE AS Model,
            DEVICETYPE.HWNAME AS Enhedstype,
            U.SERIALNUMBER AS Serienummer,
            DATEADD(HOUR, 1, DATEADD(SECOND, TRY_CAST(U.LASTONLINE AS BIGINT), '1970-01-01')) AS SidsteLoginDato,
            DATEADD(SECOND, TRY_CAST(INV2.VALUE AS BIGINT), '1970-01-01') AS SidsteRul,
            REPLACE(REPLACE(LGI.VALUE, '@LAKSEN04', ''), '@RANDERS.DK', '') AS PrimaryUser,
            BLK.VALUE AS BitlockerKode,
            BLS.VALUE AS BitlockerStatus,
            BLE.VALUE AS BitlockerKrypteringProcent,
            OSINV.VALUE AS OSVersion,
            (
                SELECT STRING_AGG(MACINV.VALUE, ',')
                FROM INV MACINV
                WHERE MACINV.UNITID = U.UNITID
                  AND MACINV.SECTION = 'Network Adapter'
                  AND MACINV.NAME LIKE 'Device #% MAC Address'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM INV IPLAN
                      WHERE IPLAN.UNITID = MACINV.UNITID
                        AND IPLAN.SECTION = 'Network Configuration'
                        AND IPLAN.NAME LIKE 'Device #% IP address'
                        AND SUBSTRING(MACINV.NAME, 9, CHARINDEX(' ', MACINV.NAME, 9) - 9) = SUBSTRING(IPLAN.NAME, 9, CHARINDEX(' ', IPLAN.NAME, 9) - 9)
                        AND (
                            IPLAN.VALUE LIKE '10.129.%'
                            OR IPLAN.VALUE LIKE '10.146.%'
                            OR IPLAN.VALUE LIKE '10.161.%'
                            OR IPLAN.VALUE LIKE '10.177.%'
                        )
                  )
            ) AS MACAdresse,
            (
                SELECT STRING_AGG(MACINV.VALUE, ',')
                FROM INV MACINV
                JOIN INV IPINV ON MACINV.UNITID = IPINV.UNITID
                    AND MACINV.SECTION = 'Network Adapter'
                    AND IPINV.SECTION = 'Network Configuration'
                    AND MACINV.NAME LIKE 'Device #% MAC Address'
                    AND IPINV.NAME LIKE 'Device #% IP address'
                    AND SUBSTRING(MACINV.NAME, 9, CHARINDEX(' ', MACINV.NAME, 9) - 9) = SUBSTRING(IPINV.NAME, 9, CHARINDEX(' ', IPINV.NAME, 9) - 9)
                    AND (
                        IPINV.VALUE LIKE '10.129.%'
                        OR IPINV.VALUE LIKE '10.146.%'
                        OR IPINV.VALUE LIKE '10.161.%'
                        OR IPINV.VALUE LIKE '10.177.%'
                    )
                WHERE MACINV.UNITID = U.UNITID
                  AND MACINV.SECTION = 'Network Adapter'
                  AND MACINV.NAME LIKE 'Device #% MAC Address'
            ) AS LanMACAdresse
        FROM UNIT U
        LEFT JOIN INV ON U.UNITID = INV.UNITID
            AND INV.SECTION = 'System'
            AND INV.NAME = 'Manufacturer'
        LEFT JOIN CSI ON U.UNITID = CSI.UNITID
            AND CSI.SECTION = 'Randers Kommune'
            AND CSI.NAME = 'WSName'
        LEFT JOIN DEVICETYPE ON U.DEVICETYPEID = DEVICETYPE.ID
        LEFT JOIN INV INV2 ON U.UNITID = INV2.UNITID
            AND INV2.SECTION = 'Operating System'
            AND INV2.NAME = 'InstallDate'
        LEFT JOIN LGI ON U.UNITID = LGI.UNITID
            AND LGI.SECTION = 'Current Logon'
            AND LGI.NAME = 'User Name'
        LEFT JOIN CSI BLK ON U.UNITID = BLK.UNITID
            AND BLK.SECTION = 'CapaServices | CapaBitLocker'
            AND (
                BLK.NAME = 'Recovery Password C: #1 Password'
                OR BLK.NAME = 'Recovery Password D: #1 Password'
                OR BLK.NAME = 'Recovery Password E: #1 Password'
            )
        LEFT JOIN CSI BLS ON U.UNITID = BLS.UNITID
            AND BLS.SECTION = 'CapaServices | CapaBitLocker'
            AND BLS.NAME = 'Protection Status C:'
        LEFT JOIN CSI BLE ON U.UNITID = BLE.UNITID
            AND BLE.SECTION = 'CapaServices | CapaBitLocker'
            AND BLE.NAME = 'Encryption Status C:'
        LEFT JOIN INV OSINV ON U.UNITID = OSINV.UNITID
            AND OSINV.SECTION = 'Operating System'
            AND OSINV.NAME = 'System'
        WHERE U.SERIALNUMBER IS NOT NULL
          AND U.TYPE = 1
    """

    logger.info(f"Executing Computer SQL command: {sql_command}")

    try:
        with capa_cms.connect() as conn:
            result = conn.execute(sql_command).fetchall()
            logger.debug(f"Computer SQL result: {result}")

        if not result:
            logger.error("No computer data found")
            return False

        six_months_ago = datetime.now() - relativedelta(months=6)

        with Session(asset_engine) as session:
            users = {u.primary_user: u for u in session.query(User).all()}

            inserted = 0
            updated = 0

            for row in result:
                (
                    unit_name, producent, model, device_type, serial_number, last_login_date, last_run, primary_user,
                    bitlocker_code, bitlocker_status, bitlocker_encryption_percentage, os_version, mac_address, lan_mac_address
                ) = row

                user_obj = users.get(primary_user)
                user_id = user_obj.user_id if user_obj else None

                drift_status = False
                if last_login_date:
                    try:
                        last_login = parse(str(last_login_date))
                        if last_login >= six_months_ago:
                            drift_status = True
                    except Exception:
                        logger.error(f"Could not parse SidsteLoginDato: {last_login_date} for {unit_name}")

                computer = session.query(Computer).filter_by(unit_name=unit_name).first()
                if computer:
                    # Update existing
                    computer.producent = producent
                    computer.model = model
                    computer.device_type = device_type
                    computer.serial_number = serial_number
                    computer.last_login_date = last_login_date
                    computer.last_run = last_run
                    computer.user_id = user_id
                    computer.bitlocker_code = bitlocker_code
                    computer.bitlocker_status = bitlocker_status
                    computer.bitlocker_encryption_percentage = bitlocker_encryption_percentage
                    computer.os_version = os_version
                    computer.drift = drift_status
                    computer.mac_address = mac_address
                    computer.lan_mac_address = lan_mac_address
                    updated += 1
                else:
                    # Insert new
                    computer = Computer(
                        unit_name=unit_name,
                        producent=producent,
                        model=model,
                        device_type=device_type,
                        serial_number=serial_number,
                        last_login_date=last_login_date,
                        last_run=last_run,
                        user_id=user_id,
                        bitlocker_code=bitlocker_code,
                        bitlocker_status=bitlocker_status,
                        bitlocker_encryption_percentage=bitlocker_encryption_percentage,
                        os_version=os_version,
                        drift=drift_status,
                        mac_address=mac_address,
                        lan_mac_address=lan_mac_address
                    )
                    session.add(computer)
                    inserted += 1

            session.commit()
            logger.info(f"Inserted {inserted}, updated {updated} computers in Computer table.")

        return True

    except Exception as e:
        logger.error(f"Error inserting computers: {e}")
        return False


def _get_atea_headers(http_hook: HttpHook) -> dict:
    """
    Build HTTP headers for Atea API using the SubKey from Airflow connection.

    :param http_hook: Airflow HttpHook for the Atea API
    :return: Dictionary of HTTP headers
    """
    conn = http_hook.get_connection(http_hook.http_conn_id)
    return {
        "Authorization": f"SubKey {conn.password}",
        "Cache-Control": "no-cache",
        "Accept": "application/json",
    }


def fetch_atea_data(http_hook: HttpHook) -> list:
    """
    Fetch asset data from Atea API.

    :param http_hook: Airflow HttpHook for the Atea API
    :return: List of asset records returned by the Atea API.
    """
    try:
        logger.info("Fetching assets from Atea API...")

        headers = _get_atea_headers(http_hook=http_hook)
        http_hook.method = "GET"

        res = http_hook.run(
            endpoint="/api/assets/search",
            data={
                "PageSize": 2000,
                "Page": 1,
                "AssetType": "AB,AA",
            },
            headers=headers,
        )

        data = res.json()

        if not isinstance(data, list):
            raise ValueError(f"Unexpected response structure: {data}")

        logger.info(f"Successfully retrieved data from Atea API. Total records: {len(data)}")
        return data

    except Exception:
        logger.exception("Error while fetching data from Atea API")
        raise


def insert_atea_data(http_hook: HttpHook, asset_engine: Engine) -> bool:
    """
    Fetch asset data from Atea API and update Computer table with price, order date, and warranty.

    :param http_hook: Airflow HttpHook for the Atea API
    :param asset_engine: SQLAlchemy Engine for the Asset DB.
    :return: True if the update succeeded, otherwise False.
    """
    try:
        atea_data = fetch_atea_data(http_hook=http_hook)
        if not atea_data:
            logger.error("No data fetched from Atea API.")
            return False

        # Map serial numbers to Atea info
        serial_info_map = {
            str(item.get('SerialNumber')).strip().lower(): {
                'price': item.get('Price'),
                'order_date': item.get('OrderDate'),
                'warranty': item.get('Warranty')
            }
            for item in atea_data
            if item.get('SerialNumber') and item.get('Price') and item.get('OrderDate') and item.get('Warranty')
        }

        with Session(asset_engine) as session:
            computers = session.query(Computer).all()
            updated = 0

            for computer in computers:
                serial_norm = str(computer.serial_number).strip().lower() if computer.serial_number else None
                info = serial_info_map.get(serial_norm)
                if info:
                    try:
                        computer.price = float(info['price'])
                    except Exception:
                        logger.warning(f"Could not convert price '{info['price']}' for serial '{serial_norm}'")
                        continue
                    computer.order_date = info['order_date']
                    computer.warranty = info['warranty']
                    updated += 1

            session.commit()
            logger.info(f"Updated price, order date, and warranty for {updated} computers from Atea API.")

        return True

    except Exception as e:
        logger.error(f"Error updating asset info from Atea: {e}")
        return False


def insert_device_license_and_historical_data(
    sftp_hook: SFTPHook,
    http_hook: HttpHook,
    asset_engine: Engine
) -> bool:
    """
    Fetch Device License CSV, Comm2ig historical CSV, and Atea EAN from SFTP,
    then update Computer table in Asset DB accordingly.

    :param sftp_hook: Airflow SFTPHook for Asset SFTP.
    :param http_hook: Airflow HttpHook for the Atea API.
    :param asset_engine: SQLAlchemy Engine for the Asset DB.
    :return: True if updates succeeded, otherwise False.
    """
    device_license_file = Variable.get("asset_config", default_var=None, deserialize_json=True)["device_license_file_path"]
    comm2ig_historical_file = Variable.get("asset_config", default_var=None, deserialize_json=True)["comm2ig_historical_file_path"]
    ean_atea_file = Variable.get("asset_config", default_var=None, deserialize_json=True)["ean_atea_file_path"]

    try:
        with sftp_hook.get_conn() as sftp_client:
            logger.info("Fetching Device License CSV from SFTP...")
            with sftp_client.open(device_license_file, 'r') as file:
                device_license_csv = file.read().decode('utf-8')

            logger.info("Fetching Comm2ig historical CSV from SFTP...")
            with sftp_client.open(comm2ig_historical_file, 'r') as file:
                comm2ig_csv = file.read().decode('utf-8')

            logger.info("Fetching EAN Atea file from SFTP...")
            with sftp_client.open(ean_atea_file, 'rb') as file:
                ean_atea_bytes = file.read()

        df_device_license = pd.read_csv(io.StringIO(device_license_csv))
        df_device_license.columns = df_device_license.columns.str.strip()
        if 'Name' not in df_device_license.columns:
            logger.error("Device License CSV missing 'Name' column.")
            return False
        computer_names = [name.strip() for name in df_device_license['Name'].dropna()]

        df_comm2ig = pd.read_csv(io.StringIO(comm2ig_csv), dtype=str, sep=',')
        df_comm2ig.columns = df_comm2ig.columns.str.strip()
        required_cols = ['Serienr.', 'Pris pr.stk. i kr. ekskl. moms', 'Fakturadato', 'EAN-nr.']
        missing = [col for col in required_cols if col not in df_comm2ig.columns]
        if missing:
            logger.error(f"Comm2ig CSV missing required columns: {missing}")
            return False

        df_atea = pd.read_excel(io.BytesIO(ean_atea_bytes), dtype=str)
        df_atea.columns = df_atea.columns.str.strip()
        if 'Nummer' not in df_atea.columns or 'EAN-nr.' not in df_atea.columns:
            logger.error("Atea Excel missing 'Nummer' or 'EAN-nr.' columns.")
            return False

        atea_data = fetch_atea_data(http_hook=http_hook)
        if not atea_data:
            logger.error("No data fetched from Atea API.")
            return False

        billto_map = {
            str(item.get('BillTo')): item.get('SerialNumber')
            for item in atea_data if item.get('BillTo') and item.get('SerialNumber')
        }

        with Session(asset_engine) as session:
            computers = session.query(Computer).all()
            name_to_computer = {c.unit_name: c for c in computers if c.unit_name}
            serial_to_computer = {str(c.serial_number).lstrip('sS').lower(): c for c in computers if c.serial_number}
            serial_exact_lookup = {str(c.serial_number): c for c in computers if c.serial_number}

            # DeviceLicense/AD
            updated_device = 0
            for name in computer_names:
                computer = name_to_computer.get(name)
                if computer:
                    computer.device_license = True
                    updated_device += 1

            # Comm2ig historisk data
            updated_comm2ig = 0
            for _, row in df_comm2ig.iterrows():
                serial = row['Serienr.']
                serial_norm = str(serial[1:]).lower() if str(serial).startswith('S') else str(serial).lower()
                price = row['Pris pr.stk. i kr. ekskl. moms']
                fakturadato = row['Fakturadato']
                ean_nr = row.get('EAN-nr.', None)
                if pd.isna(ean_nr) or str(ean_nr).strip().lower() in ['nan', '']:
                    ean_nr = None

                computer_obj = serial_to_computer.get(serial_norm)
                if computer_obj:
                    try:
                        computer_obj.price = float(str(price).replace(',', '.'))
                    except Exception:
                        logger.warning(f"Could not convert price '{price}' for serial '{serial_norm}'")
                        continue
                    computer_obj.order_date = fakturadato
                    computer_obj.kob_ean_nr = ean_nr
                    updated_comm2ig += 1

            # Atea KøbsEANnr
            updated_atea = 0
            for _, row in df_atea.iterrows():
                nummer = str(row['Nummer']).strip()
                ean_nr = row['EAN-nr.']
                serial = billto_map.get(nummer)
                computer_obj = serial_exact_lookup.get(serial)
                if serial and computer_obj:
                    computer_obj.kob_ean_nr = ean_nr
                    updated_atea += 1

            session.commit()

            logger.info(f"Device License updated for {updated_device} computers")
            logger.info(f"Comm2ig historical data updated for {updated_comm2ig} computers")
            logger.info(f"Atea kob_ean_nr updated for {updated_atea} computers")

        return True

    except Exception as e:
        logger.error(f"Error updating Device License, Comm2ig, or Atea data: {e}")
        return False
