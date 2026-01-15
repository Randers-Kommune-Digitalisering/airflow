import logging

from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse
from dag_asset.model import Base, Department, User, Computer


logger = logging.getLogger(__name__)


def create_asset_tables(db_engine: Engine) -> bool:
    """
    Create asset tables if they do not exist.
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
    Fetch users and department relations from CAPA DB and store them in asset DB.
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
    Fetch computer data from CAPA DB and store/update in asset DB.
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
