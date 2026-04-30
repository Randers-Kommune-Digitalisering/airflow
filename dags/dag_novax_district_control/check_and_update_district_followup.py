import logging
from datetime import datetime
from sqlalchemy.orm import Session
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

from dag_novax_district_control.clients.dataforsyning_client import DataforsyningClient
from dag_novax_district_control.clients.district_map_client import DistrictMapDBClient
from dag_novax_district_control.clients.cpr_client import CPRClient
from dag_novax_district_control.model import Name, NameDetails, Address, PersonDistrict, Remind

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

        sentinel_open_end = datetime(1753, 1, 1)
        sentinel_open_end_date = sentinel_open_end.date()

        invalid_entries = []
        for entry in entries:
            # CPR validation
            if not (entry.CPR.isdigit() and len(entry.CPR) == 10):
                logger.warning(
                    "Skipping Name ID %s: invalid CPR value",
                    entry.ID
                )
                invalid_entries.append(entry.ID)
                continue

            # CPR lookup: address UUID + protected status
            cpr_info = cpr_client.get_address_uuid_and_protected_status(entry.CPR)

            has_changed_protected_status = False
            prev_protected = bool(entry.details.BESKYTTETADRESSE)
            if cpr_info["is_protected_address"] != prev_protected:
                entry.details.BESKYTTETADRESSE = int(cpr_info["is_protected_address"])
                has_changed_protected_status = True
                logger.info(f"Updated protected address status for Name ID {entry.ID}")

            address_info = dataforsyning_client.get_address_by_id(cpr_info["address_uuid"])

            # Address + district updates
            is_new_address_set = False
            is_new_district = False
            is_new_district_details = False
            is_new_kommunekode = False
            is_new_kommunekode_details = False

            if address_info is None:
                logger.warning(
                    "Skipping address lookup and clearing district for Name ID %s due to unexpected Dataforsyning results (adresse_uuid=%s)",
                    entry.ID,
                    cpr_info["address_uuid"],
                )

                # Reset district to empty if address cannot be found
                for d in entry.person_districts:
                    if d.DATETO is None or d.DATETO == sentinel_open_end_date:
                        d.DATETO = now_dt
                        d.TS_UPDD = now_dt
                        d.TS_UPDT = now_time

                if entry.DISTRIKT != "":
                    entry.DISTRIKT = ""
                    is_new_district = True

                if entry.details.TS_KOMID != "":
                    entry.details.TS_KOMID = ""
                    is_new_kommunekode_details = True

                if is_new_district or is_new_kommunekode_details:
                    logger.info(
                        "Cleared district for Name ID %s due to missing address",
                        entry.ID,
                    )

            else:
                # Address update
                new_full_address = address_info.get("full_address")
                if new_full_address != entry.ADRESSE:
                    is_new_address_set = True
                    entry.ADRESSE = new_full_address
                    logger.info(f"Updated address for Name ID {entry.ID}")

                    has_valid_address = any(
                        a.NR_LT_ETAGE == address_info.get("number_floor") and
                        a.VEJKODE == address_info.get("street_code") and
                        a.STEDNAVN == address_info.get("town_name") and
                        a.POSTNR == address_info.get("postal_code") and
                        a.KOMMUNEKODE == address_info.get("municipality_code") and
                        (a.DATO_FRA is not None and a.DATO_FRA <= today) and
                        (
                            a.DATO_TIL is None or
                            a.DATO_TIL == sentinel_open_end_date or
                            a.DATO_TIL > today
                        )
                        for a in entry.addresses
                    )

                    if not has_valid_address:
                        for a in entry.addresses:
                            if a.DATO_TIL is None or a.DATO_TIL == sentinel_open_end_date:
                                a.DATO_TIL = now_dt
                                a.TS_UPDD = now_dt
                                a.TS_UPDT = now_time
                                logger.info(
                                    f"Closed existing address {a.VEJKODE} {a.NR_LT_ETAGE} for Name ID {entry.ID} with end date {today}"
                                )
                        new_address_entry = Address(
                            NAVNID=entry.ID,
                            VEJKODE=str(address_info["street_code"]),
                            KOMMUNEKODE=str(address_info["municipality_code"]),
                            POSTNR=str(address_info["postal_code"]),
                            STEDNAVN=address_info["town_name"],
                            NR_LT_ETAGE=address_info["number_floor"],
                            DATO_FRA=now_dt,
                            DATO_TIL=sentinel_open_end,
                            TS_DATE=now_dt,
                            TS_TIME=now_time,
                            TS_UPDD=now_dt,
                            TS_UPDT=now_time,
                        )
                        entry.addresses.append(new_address_entry)
                        logger.info(f"Added new address for Name ID {entry.ID}")

                    # Notify ansvarShpl if address has changed
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

                if new_district and new_district != entry.DISTRIKT:
                    is_new_district = True
                    entry.DISTRIKT = new_district
                    logger.info(f"Updated district for Name ID {entry.ID}")

                    has_valid_person_district = any(
                        d.DISTRICT == new_district and
                        (d.DATEFROM is not None and d.DATEFROM <= today) and
                        (
                            d.DATETO is None or
                            d.DATETO == sentinel_open_end_date or
                            d.DATETO > today
                        )
                        for d in entry.person_districts
                    )

                    if not has_valid_person_district:
                        for d in entry.person_districts:
                            if d.DATETO is None or d.DATETO == sentinel_open_end_date:
                                d.DATETO = now_dt
                                d.TS_UPDD = now_dt
                                d.TS_UPDT = now_time
                                logger.info(
                                    f"Closed existing person district {d.DISTRICT} for Name ID {entry.ID} with end date {today}"
                                )
                        new_person_district = PersonDistrict(
                            NAVNID=entry.ID,
                            DISTRICT=new_district,
                            DATEFROM=now_dt,
                            DATETO=sentinel_open_end,
                            TS_DATE=now_dt,
                            TS_TIME=now_time,
                            TS_UPDD=now_dt,
                            TS_UPDT=now_time,
                        )
                        entry.person_districts.append(new_person_district)
                        logger.info(f"Added new person district for Name ID {entry.ID}: {new_district}")

                # Kommune update in name and name details
                if entry.TS_KOMID != "730":
                    entry.TS_KOMID = "730"
                    is_new_kommunekode = True

                if entry.details.TS_KOMID != "730" or entry.details.KOMMUNE_OPR != "730":
                    entry.details.TS_KOMID = "730"
                    entry.details.KOMMUNE_OPR = "730"
                    is_new_kommunekode_details = True

            # Always update active status
            has_changed_active = False
            if entry.AKTIV in ("", "0"):
                entry.AKTIV = "1"
                has_changed_active = True
                logger.info(f"Updated active status for Name ID {entry.ID}")

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
