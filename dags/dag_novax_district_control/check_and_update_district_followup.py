import logging
import datetime
from sqlalchemy.orm import Session
from sqlalchemy import or_, and_
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

from dag_novax_district_control.run_utils_followup import determine_run_date, followup_due_date_windows
from dag_novax_district_control.clients.dataforsyning_client import DataforsyningClient
from dag_novax_district_control.clients.district_map_client import DistrictMapDBClient
from dag_novax_district_control.clients.cpr_client import CPRClient
from dag_novax_district_control.model import Name, NameDetails, Address, PersonDistrict

logger = logging.getLogger(__name__)

def check_and_update_district_followup(dry_run: bool) -> None:
    """
    Retrieves and updates user, address and district information
    for any patients with an upcoming due date based on their addresses.
    """
    # Initialize clients
    dataforsyning_client = DataforsyningClient()
    district_db_client = DistrictMapDBClient()
    cpr_client = CPRClient()

    run_date = determine_run_date()
    windows = followup_due_date_windows(run_date=run_date, months_ahead=9)
    logger.info(
        "Followup run_date=%s; querying due dates in %s window(s): %s",
        run_date,
        len(windows),
        "; ".join([f"[{w.start}..{(w.end - datetime.timedelta(days=1))}]" for w in windows]),
    )

    hook = MsSqlHook(mssql_conn_id="novax_sql")
    engine = hook.get_sqlalchemy_engine()

    # Query patients with due dates in any of the windows (end is exclusive)
    window_predicates = [
        and_(NameDetails.TERMIN >= w.start, NameDetails.TERMIN < w.end)
        for w in windows
    ]

    with Session(engine) as session:
        entries: list[Name] = (
            session.query(Name)
            .join(NameDetails, NameDetails.NAVNID == Name.ID)
            .filter(or_(*window_predicates))
            .order_by(Name.ID)
            .all()
        )

        logger.info("Found %s patients with due dates in followup windows", len(entries))
        if not entries:
            logger.info("No matching due dates for this run; exiting.")
            return

        logger.info("Processing %s patients for district/address follow-up", len(entries))

        sentinel_open_end = datetime.datetime(1753, 1, 1)
        now_dt = datetime.datetime.now()
        now_time = now_dt.strftime("%H:%M")

        for entry in entries:
            detected_changes: list[str] = []

            if entry.details is None:
                logger.warning("Skipping Name ID %s: missing NameDetails", entry.ID)
                continue

            # CPR lookup: address UUID + protected status
            cpr_info = cpr_client.get_address_uuid_and_protected_status(entry.CPR)

            has_changed_protected_status = False
            prev_protected = bool(entry.details.BESKYTTETADRESSE)
            if cpr_info["is_protected_address"] != prev_protected:
                entry.details.BESKYTTETADRESSE = int(cpr_info["is_protected_address"])
                has_changed_protected_status = True
                detected_changes.append(f"protected: {prev_protected} -> {cpr_info['is_protected_address']}")

            address_info = dataforsyning_client.get_address_by_id(cpr_info["address_uuid"])

            # Address update + address history
            is_new_address_set = False
            new_full_address = address_info["full_address"].strip()
            if new_full_address != (entry.ADRESSE or "").strip():
                is_new_address_set = True
                old_address = (entry.ADRESSE or "").strip() or None
                entry.ADRESSE = new_full_address
                detected_changes.append(f"address: {old_address} -> {new_full_address}")

                has_valid_address = any(
                    (a.NR_LT_ETAGE or "").strip() == (address_info["number_floor"] or "").strip()
                    and int((a.VEJKODE or "0").strip() or 0) == int(address_info["street_code"])
                    and (a.STEDNAVN or None) == address_info["town_name"]
                    and int((a.POSTNR or "0").strip() or 0) == int(address_info["postal_code"])
                    and int((a.KOMMUNEKODE or "0").strip() or 0) == int(address_info["municipality_code"])
                    and a.DATO_FRA.date() <= now_dt.date()
                    and (
                        a.DATO_TIL.date() == sentinel_open_end.date()
                        or a.DATO_TIL.date() > now_dt.date()
                    )
                    for a in entry.addresses
                )

                if not has_valid_address:
                    for a in entry.addresses:
                        if a.DATO_TIL.date() == sentinel_open_end.date():
                            a.DATO_TIL = now_dt
                            a.TS_UPDD = now_dt
                            a.TS_UPDT = now_time
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

            # District update + district history
            district = district_db_client.get_district_name_for_point(
                x=address_info["coordinates"][0],
                y=address_info["coordinates"][1],
            )

            is_new_district = False
            is_new_district_details = False
            if district and district.strip() != (entry.DISTRIKT or "").strip():
                is_new_district = True
                old_district = (entry.DISTRIKT or "").strip() or None
                entry.DISTRIKT = district.strip()
                detected_changes.append(f"district: {old_district} -> {district.strip()}")

                if (entry.details.TS_KOMID or "").strip() != district.strip():
                    entry.details.TS_KOMID = district.strip()
                    is_new_district_details = True

                has_valid_person_district = any(
                    (d.DISTRICT or "").strip() == district.strip()
                    and d.DATEFROM.date() <= now_dt.date()
                    and (
                        d.DATETO.date() == sentinel_open_end.date()
                        or d.DATETO.date() > now_dt.date()
                    )
                    for d in entry.person_districts
                )
                if not has_valid_person_district:
                    for d in entry.person_districts:
                        if d.DATETO and d.DATETO.date() == sentinel_open_end.date():
                            d.DATETO = now_dt
                            d.TS_UPDD = now_dt
                            d.TS_UPDT = now_time
                    new_person_district = PersonDistrict(
                        NAVNID=entry.ID,
                        DISTRICT=district.strip(),
                        DATEFROM=now_dt,
                        DATETO=sentinel_open_end,
                        TS_DATE=now_dt,
                        TS_TIME=now_time,
                        TS_UPDD=now_dt,
                        TS_UPDT=now_time,
                    )
                    entry.person_districts.append(new_person_district)

            # Always-updates to match main job expectations
            has_changed_active = False
            if entry.AKTIV is None or str(entry.AKTIV).strip() in ("", "0"):
                entry.AKTIV = "1"
                has_changed_active = True
                detected_changes.append("aktiv: 0/empty -> 1")

            has_changed_ansvarshpl = False
            if entry.AnsvarsShpl != "FIKTIV":
                old_val = entry.AnsvarsShpl
                entry.AnsvarsShpl = "FIKTIV"
                has_changed_ansvarshpl = True
                detected_changes.append(f"ansvarshpl: {old_val} -> FIKTIV")

            if detected_changes:
                logger.info("Detected changes for navnid %s: %s", entry.ID, ", ".join(detected_changes))
            else:
                logger.info("No changes detected for navnid %s", entry.ID)

            if any([is_new_district, is_new_address_set, has_changed_active, has_changed_ansvarshpl]):
                entry.TS_UPDD = now_dt
                entry.TS_UPDT = now_time

            if any([has_changed_protected_status, is_new_district_details]):
                entry.details.TS_UPDD = now_dt
                entry.details.TS_UPDT = now_time

        if dry_run:
            logger.warning("Dry run enabled - no changes committed to the database")
        else:
            logger.info("Committing changes to the database")
            session.commit()

    logger.info("Successfully completed check_and_update_district_followup")
