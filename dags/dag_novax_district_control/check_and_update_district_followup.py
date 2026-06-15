import logging
from datetime import datetime
from sqlalchemy.orm import Session
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

from dag_novax_district_control.clients.dataforsyning_client import DataforsyningClient
from dag_novax_district_control.clients.district_map_client import DistrictMapDBClient
from dag_novax_district_control.clients.cpr_client import CPRClient
from dag_novax_district_control.model import Name, NameDetails, Remind
from dag_novax_district_control.district_update_helpers import (
    ensure_active,
    is_valid_cpr,
    update_address_from_dataforsyning,
    update_district_from_coordinates,
    update_kommunekode,
    update_protected_address_status,
)

logger = logging.getLogger(__name__)


def check_and_update_district_followup(dry_run: bool, ignore_cprs: list, **context) -> None:
    """
    Retrieves and updates user, address and district information
    for any patients with an upcoming due date based on their addresses.
    """
    now_dt = datetime.now()
    now_time = now_dt.strftime("%H:%M")
    today = now_dt.date()
    run_dt = datetime.combine(today, datetime.min.time())
    logger.info("Querying all upcoming due dates from today (TERMIN >= %s)", today)

    # Initialize clients
    dataforsyning_client = DataforsyningClient()
    district_db_client = DistrictMapDBClient()
    cpr_client = CPRClient()

    # Novax session
    hook = MsSqlHook(mssql_conn_id="novax_sql")
    engine = hook.get_sqlalchemy_engine()

    # Query patients with upcoming due dates
    with Session(engine) as session:
        entries: list[Name] = (
            session.query(Name)
            .join(
                NameDetails,
                NameDetails.NAVNID == Name.ID
            )
            .filter(
                NameDetails.TERMIN >= run_dt,
                Name.CPR.not_in(ignore_cprs)
            )
            .order_by(Name.ID)
            .all()
        )

        if not entries:
            logger.info("No matching due dates for this run; exiting.")
            return

        logger.info("Processing %s patients for district/address follow-up", len(entries))

        invalid_entries = []
        for entry in entries:
            # CPR validation
            if not is_valid_cpr(entry.CPR):
                logger.warning(
                    "Skipping Name ID %s: invalid CPR value",
                    entry.ID
                )
                invalid_entries.append(entry.ID)
                continue

            # CPR lookup: address UUID + protected status
            cpr_info = cpr_client.get_address_uuid_and_protected_status(entry.CPR)

            has_changed_protected_status = update_protected_address_status(
                entry=entry,
                is_protected_address=bool(cpr_info["is_protected_address"]),
            )

            address_uuid = cpr_info['address_uuid']
            address_info = None
            if address_uuid is not None:
                address_info = dataforsyning_client.get_address_by_id(address_uuid)

            # Address + district updates
            is_new_address_set = False
            is_new_district = False
            is_new_district_details = False
            is_new_kommunekode = False
            is_new_kommunekode_details = False

            if address_info is None:
                logger.warning(
                    "Skipping address lookup + district update for Name ID %s due to unexpected address information (address_uuid=%s)",
                    entry.ID,
                    address_uuid,
                )

            else:
                # Address update
                new_full_address = address_info.get("full_address")
                is_new_address_set = update_address_from_dataforsyning(
                    entry=entry,
                    address_info=address_info,
                    reference_date=today,
                    close_to_dt=now_dt,
                    new_from_dt=now_dt,
                    now_dt=now_dt,
                    now_time=now_time,
                )

                if is_new_address_set:
                    new_reminder = Remind(
                        NAVNID=entry.ID,
                        KODE='FLYTTET',
                        BEMAERK=f"Er flyttet til ny adresse: {new_full_address}",
                        BRUGER=entry.AnsvarsShpl,
                        TS_DATE=now_dt,
                        TS_TIME=now_time,
                        TS_UPDD=now_dt,
                        TS_UPDT=now_time,
                        OPRETTET=now_dt
                    )
                    session.add(new_reminder)
                    logger.info(f"Added reminder to {entry.AnsvarsShpl.strip()} for Name ID {entry.ID}")

                # District update
                new_district = district_db_client.get_district_name_for_point(
                    x=address_info["coordinates"][0],
                    y=address_info["coordinates"][1],
                )

                is_new_district = update_district_from_coordinates(
                    entry=entry,
                    new_district=new_district,
                    reference_date=today,
                    close_to_dt=now_dt,
                    new_from_dt=now_dt,
                    now_dt=now_dt,
                    now_time=now_time,
                )

                # Kommune update in name and name details
                is_new_kommunekode, is_new_kommunekode_details = update_kommunekode(entry=entry)

            # Always update active status
            has_changed_active = ensure_active(entry=entry)

            if any([is_new_district, is_new_address_set, has_changed_active, is_new_kommunekode]):
                entry.TS_UPDD = now_dt
                entry.TS_UPDT = now_time

            if any([has_changed_protected_status, is_new_district_details, is_new_kommunekode_details]):
                entry.details.TS_UPDD = now_dt
                entry.details.TS_UPDT = now_time

        if dry_run:
            logger.warning("Dry run enabled - no changes committed to the database")
        else:
            logger.info("Committing changes to the database")
            session.commit()

    if invalid_entries:
        logger.error(f"Entries with invalid CPR values that were skipped: {invalid_entries}")
        raise ValueError(f"Invalid CPR values found for Name IDs: {invalid_entries}")
    else:
        logger.info("Successfully completed check_and_update_district_followup")
