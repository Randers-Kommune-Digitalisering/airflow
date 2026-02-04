import logging
import pandas as pd

from sqlalchemy.engine import Engine
from sqlalchemy import text, select
from sqlalchemy.orm import Session
from datetime import datetime
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.sftp.hooks.sftp import SFTPHook
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse
from dag_asset.model import Base, Department, User, Computer
from airflow.models import Variable
from utils.utils import df_to_csv_bytes
from rkdigi.syncpkg.token_session import ManagedOAuth2Session


logger = logging.getLogger(__name__)


def create_asset_tables(db_engine: Engine) -> bool:
    """
    Create asset database tables if they do not exist.

    :param db_engine: SQLAlchemy Engine for the Asset DB.
    :return: True if table creation succeeded.
    """
    Base.metadata.create_all(db_engine)
    logger.info("Created Computer, User, and Department tables if not exists.")
    return True


def insert_departments_data(capa_cms_engine: Engine, asset_engine: Engine) -> bool:
    """
    Fetch departments from CAPA DB and store them in asset DB.

    :param capa_cms_engine: SQLAlchemy Engine for the CAPA CMS DB.
    :param asset_engine: SQLAlchemy Engine for the Asset DB.
    """
    sql_command = """
        SELECT DISTINCT USI.VALUE AS DEPARTMENT
        FROM USI
        WHERE USI.SECTION = 'General User Inventory'
          AND USI.NAME = 'Department'
    """

    logger.debug(f"Executing Department SQL command: {sql_command}")

    with capa_cms_engine.connect() as conn:
        departments = conn.execute(text(sql_command)).scalars().all()
        logger.debug(f"Department SQL result: {departments}")

    if not departments:
        raise ValueError("No department data found in CAPA DB")

    with Session(asset_engine) as session:
        existing_departments = {
            str(name).strip().lower()
            for name in session.execute(select(Department.name)).scalars().all()
            if name
        }

        inserted = 0
        for department in departments:
            if not isinstance(department, str):
                continue

            department_norm = department.strip().lower()
            if not department_norm:
                continue

            if department_norm not in existing_departments:
                session.add(Department(name=department_norm))
                existing_departments.add(department_norm)
                inserted += 1

        session.commit()

    logger.info(f"Inserted {inserted} unique departments into Department table.")
    return True


def insert_users_data(capa_cms_engine: Engine, asset_engine: Engine) -> bool:
    """
    Fetch users and department relations from CAPA DB and store them in Asset DB.
    """

    sql_command = """
        WITH Users AS (
            SELECT DISTINCT
                UNIT.UNITID,
                LEFT(LGI.VALUE, CHARINDEX('@', LGI.VALUE + '@') - 1) AS USERNAME
            FROM UNIT
            JOIN LGI ON UNIT.UNITID = LGI.UNITID
            WHERE LGI.SECTION = 'Current Logon'
              AND LGI.NAME = 'User Name'
        )
        SELECT DISTINCT
            U.USERNAME,
            USI.VALUE AS FULLNAME,
            LOWER(LTRIM(RTRIM(USI2.VALUE))) AS DEPARTMENT
        FROM Users U
        JOIN UNIT ON UNIT.NAME = U.USERNAME
        JOIN USI ON UNIT.UNITID = USI.UNITID
            AND USI.SECTION = 'General User Inventory'
            AND USI.NAME = 'Full Name'
        JOIN USI USI2 ON UNIT.UNITID = USI2.UNITID
            AND USI2.SECTION = 'General User Inventory'
            AND USI2.NAME = 'Department'
    """

    logger.debug(f"Executing User SQL command: {sql_command}")

    with capa_cms_engine.connect() as conn:
        result = conn.execute(text(sql_command)).all()
        logger.debug(f"User SQL result: {result}")

    with Session(asset_engine) as session:
        departments = {
            d.name: d
            for d in session.execute(select(Department)).scalars().all()
        }

        users = {
            u.primary_user: u
            for u in session.execute(select(User)).scalars().all()
        }

        new_users: dict[str, User] = {}

        # Add new users (bulk)
        for username, fullname, _ in result:
            if username not in users and username not in new_users:
                new_users[username] = User(
                    primary_user=username,
                    full_name=fullname
                )

        if new_users:
            session.add_all(new_users.values())
            session.flush()
            users.update(new_users)

        # Attach departments to users
        for username, _, department in result:
            user = users.get(username)
            dept = departments.get(department)

            if user and dept and dept not in user.departments:
                user.departments.append(dept)

        session.commit()

    logger.info(
        f"Inserted {len(new_users)} new users and linked departments.")
    return True


def insert_computers_data(capa_cms_engine: Engine, asset_engine: Engine) -> bool:
    """
    Fetch computer data from CAPA DB and store/update in Asset DB.

    :param capa_cms_engine: SQLAlchemy Engine for the CAPA CMS DB.
    :param asset_engine: SQLAlchemy Engine for the Asset DB.
    :return: True if computers were inserted/updated successfully, otherwise False.
    """
    sql_command = """
        SELECT
            U.NAME AS unit_name,
            INV.VALUE AS producent,
            CSI.VALUE AS model,
            DEVICETYPE.HWNAME AS device_type,
            U.SERIALNUMBER AS serial_number,
            DATEADD(HOUR, 1, DATEADD(SECOND, TRY_CAST(U.LASTONLINE AS BIGINT), '1970-01-01')) AS last_login_date,
            DATEADD(SECOND, TRY_CAST(INV2.VALUE AS BIGINT), '1970-01-01') AS last_run,
            LEFT(LGI.VALUE, CHARINDEX('@', LGI.VALUE + '@') - 1) AS primary_user,
            BLK.VALUE AS bitlocker_code,
            BLS.VALUE AS bitlocker_status,
            BLE.VALUE AS bitlocker_encryption_percentage,
            OSINV.VALUE AS os_version,
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
            ) AS mac_address,
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
            ) AS lan_mac_address
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

    logger.debug(f"Executing Computer SQL command: {sql_command}")

    with capa_cms_engine.connect() as conn:
        rows = conn.execute(text(sql_command)).mappings().all()

    if not rows:
        raise ValueError("No computer data found in CAPA CMS DB")

    six_months_ago = datetime.now() - relativedelta(months=6)

    with Session(asset_engine) as session:
        users = {u.primary_user: u for u in session.execute(select(User)).scalars().all()}

        inserted = 0
        updated = 0

        for row_map in rows:
            row = dict(row_map)

            primary_user = row.pop("primary_user", None)
            user_obj = users.get(primary_user) if primary_user else None
            row["user_id"] = user_obj.user_id if user_obj else None

            last_login_date = row.get("last_login_date")
            drift_status = False
            if last_login_date:
                try:
                    drift_status = parse(str(last_login_date)) >= six_months_ago
                except Exception:
                    logger.error(
                        f"Could not parse last_login_date: {last_login_date} for {row.get('unit_name')}"
                    )
            row["drift"] = drift_status

            unit_name = row.get("unit_name")
            if not unit_name:
                continue

            computer = session.query(Computer).filter_by(unit_name=unit_name).first()
            if computer:
                for k, v in row.items():
                    setattr(computer, k, v)
                updated += 1
            else:
                session.add(Computer(**row))
                inserted += 1

        session.commit()
        logger.info(f"Inserted {inserted}, updated {updated} computers in Computer table.")
        return True


def _fetch_atea_data(http_hook: HttpHook) -> list:
    """
    Fetch asset data from Atea API
    """
    logger.info("Fetching assets from Atea API ...")

    conn = http_hook.get_connection(http_hook.http_conn_id)
    if not conn.password:
        raise ValueError("Missing SubKey (connection password) for Atea API connection.")

    headers = {
        "Authorization": f"SubKey {conn.password}",
    }

    http_hook.method = "GET"

    page_size = 1000
    page = 1
    all_rows: list = []

    while True:
        logger.info(f"Fetching Atea assets page={page} page_size={page_size} ...")

        res = http_hook.run(
            endpoint="/api/assets/search",
            data={"PageSize": page_size, "Page": page, "AssetType": "AB,AA"},
            headers=headers,
        )

        data = res.json()

        all_rows.extend(data)

        if len(data) < page_size:
            break

        page += 1

    logger.info(f"Successfully retrieved data from Atea API. Total records: {len(all_rows)} (pages fetched: {page})")
    return all_rows


def insert_atea_data(http_hook: HttpHook, asset_engine: Engine) -> bool:
    """
    Fetch asset data from Atea API and update Computer table with price, order date, and warranty.

    :param http_hook: Airflow HttpHook for the Atea API
    :param asset_engine: SQLAlchemy Engine for the Asset DB.
    :return: True if the update succeeded, otherwise False.
    """
    atea_data = _fetch_atea_data(http_hook=http_hook)
    if not atea_data:
        logger.error("No data fetched from Atea API.")
        return False

    # Map serial numbers to Atea info
    serial_info_map = {
        item['SerialNumber']: {
            'price': item['Price'],
            'order_date': item['OrderDate'],
            'warranty': item['Warranty'],
        }
        for item in atea_data
        if all(
            item.get(key)
            for key in ('SerialNumber', 'Price', 'OrderDate', 'Warranty')
        )
    }

    with Session(asset_engine) as session:
        computers = session.query(Computer).all()
        updated = 0

        for computer in computers:
            if not computer.serial_number:
                continue

            serial_norm = computer.serial_number
            info = serial_info_map.get(serial_norm)

            if not info:
                continue

            computer.price = info['price']
            computer.order_date = info['order_date']
            computer.warranty = info['warranty']
            updated += 1

        session.commit()
        logger.info(
            f"Updated price, order date, and warranty for {updated} computers from Atea API.")

    return True


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

    with sftp_hook.get_conn() as sftp_client:
        logger.info("Fetching Device License CSV from SFTP...")
        with sftp_client.open(device_license_file, 'r') as file:
            df_device_license = pd.read_csv(
                file,
                usecols=['Name']
            )
            df_device_license.columns = df_device_license.columns.str.strip()

        logger.info("Fetching Comm2ig historical CSV from SFTP...")
        with sftp_client.open(comm2ig_historical_file, 'r') as file:
            df_comm2ig = pd.read_csv(
                file,
                usecols=['Serienr.', 'Pris pr.stk. i kr. ekskl. moms', 'Fakturadato', 'EAN-nr.'],
            )
            df_comm2ig.columns = df_comm2ig.columns.str.strip()

        logger.info("Fetching EAN Atea file from SFTP...")
        with sftp_client.open(ean_atea_file, 'rb') as file:
            df_atea = pd.read_excel(
                file,
                dtype=str,
                usecols=['Nummer', 'EAN-nr.'],
            )
            df_atea.columns = df_atea.columns.str.strip()

    # Fetch Atea API Data
    atea_data = _fetch_atea_data(http_hook=http_hook)
    if not atea_data:
        logger.error("No data fetched from Atea API.")
        return False

    atea_api_df = (
        pd.DataFrame(atea_data)[['BillTo', 'SerialNumber']]
        .dropna()
        .astype(str)
    )

    atea_file_df = df_atea[['Nummer', 'EAN-nr.']].dropna().astype(str)
    merged_atea_df = (
        pd.merge(
            atea_file_df,
            atea_api_df,
            left_on='Nummer',
            right_on='BillTo',
            how='inner'
        )
        .rename(columns={
            'SerialNumber': 'serial_number',
            'EAN-nr.': 'kob_ean_nr'
        })[['serial_number', 'kob_ean_nr']]
    )

    with Session(asset_engine) as session:
        computers = session.query(Computer).all()
        name_to_computer = {c.unit_name: c for c in computers if c.unit_name}
        serial_to_computer = {str(c.serial_number).lstrip('sS').lower(): c for c in computers if c.serial_number}
        serial_exact_lookup = {str(c.serial_number): c for c in computers if c.serial_number}
        computer_names = [name.strip() for name in df_device_license['Name'].dropna()]

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
            serial_norm = str(serial).lstrip('sS').lower()
            price = row['Pris pr.stk. i kr. ekskl. moms']
            fakturadato = row['Fakturadato']
            ean_nr = row.get('EAN-nr.', None)
            if pd.isna(ean_nr) or str(ean_nr).strip().lower() in ['nan', '']:
                ean_nr = None

            computer_obj = serial_to_computer.get(serial_norm)
            if computer_obj:
                computer_obj.price = float(str(price).replace(',', '.'))
                computer_obj.order_date = fakturadato
                computer_obj.kob_ean_nr = ean_nr
                updated_comm2ig += 1

        # Atea KøbsEANnr
        updated_atea = 0
        for _, row in merged_atea_df.iterrows():
            serial = row['serial_number']
            ean_nr = row['kob_ean_nr']

            computer_obj = serial_exact_lookup.get(serial)
            if computer_obj:
                computer_obj.kob_ean_nr = ean_nr
                updated_atea += 1

        session.commit()

        logger.info(f"Device License updated for {updated_device} computers")
        logger.info(f"Comm2ig historical data updated for {updated_comm2ig} computers")
        logger.info(f"Atea kob_ean_nr updated for {updated_atea} computers")

        return True


def _delta_get_all_adm_units_ean(token_session: ManagedOAuth2Session, base_url: str) -> list[dict]:
    """
    Query Delta system for all administrative units EAN numbers.

    :param token_session: ManagedOAuth2Session that handles OAuth2 token acquisition automatically.
    :param base_url: Base URL for the Delta API.
    :return: List of dicts with keys: 'parent' (parent name), 'name' (child name), 'ean' (EAN number or None).
    """
    offset = 0
    limit = 1000
    results = []

    url = f"{base_url.rstrip('/')}/api/object/graph-query"

    while True:
        graph_query = {
            "graphQueries": [
                {
                    "computeAvailablePages": True,
                    "graphQuery": {
                        "structure": {"alias": "adm", "userKey": "APOS-Types-AdministrativeUnit"},
                        "criteria": {
                            "type": "AND",
                            "criteria": [
                                {
                                    "type": "MATCH",
                                    "operator": "EQUAL",
                                    "left": {"source": "DEFINITION", "alias": "adm.$state"},
                                    "right": {"source": "STATIC", "value": "STATE_ACTIVE"},
                                }
                            ],
                        },
                        "projection": {
                            "identity": True,
                            "attributes": ["APOS-Types-AdministrativeUnit-Attribute-EANnr"],
                            "children": {
                                "identity": True,
                                "attributes": ["APOS-Types-AdministrativeUnit-Attribute-EANnr"],
                            },
                        },
                    },
                    "validDate": "NOW",
                    "offset": offset,
                    "limit": limit,
                }
            ]
        }

        logger.debug(f"POST URL: {url}")

        res = token_session.post(url=url, json=graph_query, timeout=30)
        res.raise_for_status()

        payload = res.json()
        graph_result = payload["graphQueryResult"][0]
        available_pages = graph_result.get("availablePages", 0)
        instances = graph_result.get("instances", [])

        for inst in instances:
            parent_name = inst.get("identity", {}).get("name")
            parent_ean = next(
                (
                    att["value"]
                    for att in inst.get("attributes", [])
                    if att.get("userKey") == "APOS-Types-AdministrativeUnit-Attribute-EANnr"
                ),
                None,
            )

            children = inst.get("childrenObjects", [])
            for child in children:
                name = child.get("identity", {}).get("name", "")
                ean = next(
                    (
                        att["value"]
                        for att in child.get("attributes", [])
                        if att.get("userKey") == "APOS-Types-AdministrativeUnit-Attribute-EANnr"
                    ),
                    None,
                )
                if not ean and parent_ean:
                    ean = parent_ean

                results.append({"parent": parent_name, "name": name, "ean": ean})

        total_instances = available_pages * limit
        if offset + limit >= total_instances or len(instances) < limit:
            break

        offset += limit

    return results


def insert_department_ean_from_delta(
    token_session: ManagedOAuth2Session,
    asset_engine: Engine,
    delta_base_url: str,
) -> bool:
    """
    Update Department EAN in the asset database using data from Delta.

    :param token_session: ManagedOAuth2Session used to call Delta API (handles OAuth2 tokens automatically).
    :param asset_engine: SQLAlchemy Engine for the Asset DB.
    :param delta_base_url: Base URL for the Delta API
    :return: True if the update succeeded, otherwise False.
    """
    try:
        logger.info("Fetching department EAN numbers from Delta...")
        delta_data = _delta_get_all_adm_units_ean(
            token_session=token_session,
            base_url=delta_base_url,
        )

        if not delta_data:
            logger.error("No departments/EAN numbers fetched from Delta.")
            return False
        logger.info(f"Found {len(delta_data)} departments with EAN numbers from Delta.")

        with Session(asset_engine) as session:
            departments = session.query(Department).all()
            department_lookup = {
                d.name.strip().lower(): d
                for d in departments
                if d.name
            }

            updated = 0
            for adm in delta_data:
                name = str(adm.get("name", "")).strip().lower()
                ean = adm.get("ean")

                if not name or not ean:
                    continue

                department = department_lookup.get(name)
                if department:
                    department.ean = str(ean).strip()
                    updated += 1

            session.commit()
            logger.info(f"Updated EAN number for {updated} departments from Delta.")

        return True

    except Exception as e:
        logger.exception(f"Error updating department EAN from Delta: {e}")
        return False


def upload_assets_to_topdesk(asset_engine: Engine, http_hook: HttpHook) -> bool:
    """
    Export asset data from the Asset DB and upload it to Topdesk API as a CSV.

    :param asset_engine: SQLAlchemy Engine for the Asset DB.
    :param http_hook: Airflow HttpHook configured for the Topdesk API
    :return: True if data was exported and uploaded successfully, otherwise False.
    """
    topdesk_asset_filename = Variable.get("asset_config", default_var=None, deserialize_json=True)["topdesk_file_path"]

    sql_command = """
        SELECT
            STRING_AGG(a."name", ', ') AS "department",
            STRING_AGG(a."ean", ', ') AS "department_ean",
            b."full_name",
            b."primary_user",
            c."unit_name",
            c."producent",
            c."model",
            c."device_type",
            c."serial_number",
            c."last_login_date",
            c."last_run",
            c."bitlocker_code",
            c."bitlocker_status",
            c."bitlocker_encryption_percentage",
            c."os_version",
            c."mac_address",
            c."lan_mac_address",
            c."device_license",
            c."price",
            c."order_date",
            c."kob_ean_nr",
            c."warranty",
            c."drift"
        FROM public."computer" c
        LEFT JOIN public."user" b ON c."user_id" = b."user_id"
        LEFT JOIN public."user_department" ba ON b."user_id" = ba."user_id"
        LEFT JOIN public."department" a ON ba."department_id" = a."department_id"
        GROUP BY
            b."full_name",
            b."primary_user",
            c."unit_name",
            c."producent",
            c."model",
            c."device_type",
            c."serial_number",
            c."last_login_date",
            c."last_run",
            c."bitlocker_code",
            c."bitlocker_status",
            c."bitlocker_encryption_percentage",
            c."os_version",
            c."mac_address",
            c."lan_mac_address",
            c."device_license",
            c."price",
            c."order_date",
            c."kob_ean_nr",
            c."warranty",
            c."drift"
    """

    logger.debug(f"Executing all asset data SQL command: {sql_command}")

    with asset_engine.connect() as conn:
        result = conn.execute(text(sql_command)).mappings().all()

    if not result:
        logger.error("No data found in Computer/User/Department tables")
        return False

    df = pd.DataFrame(result)

    # Transform data to match TopDesk requirements
    for col in ["last_login_date", "last_run", "order_date", "warranty"]:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda val: "" if pd.isnull(val) else pd.to_datetime(val).strftime("%Y-%m-%dT%H:%M:%S.00")
                if str(val).strip() else str(val)
            )

    for col in ["drift", "device_license"]:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda val: "TRUE" if val is True or str(val).lower() == "true" else ""
            )

    if "price" in df.columns:
        df["price"] = df["price"].apply(
            lambda val: "{:.2f}".format(float(val)) if pd.notnull(val) and str(val).strip() else ""
        )

    logger.info(f"File name: {topdesk_asset_filename}")

    csv_bytes = df_to_csv_bytes(df, sep=';', encoding='UTF-8')
    upload_path = f"/services/import-to-api-v1/api/sourceFiles?filename={topdesk_asset_filename}"

    logger.info(f"Uploading {topdesk_asset_filename} to TopDesk {upload_path}")

    http_hook.method = "PUT"
    res = http_hook.run(
        endpoint=upload_path,
        data=csv_bytes,
    )

    logger.info(f"Successfully uploaded {topdesk_asset_filename} to TopDesk: Code Status: {res.status_code}.")

    return True
