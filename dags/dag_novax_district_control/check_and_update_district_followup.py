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
from dag_novax_district_control.utils import _i, _s, _to_date

logger = logging.getLogger(__name__)


def check_and_update_district_followup(dry_run: bool) -> None:
    """
    Retrieves and updates user, address and district information
    for any patients with an upcoming due date based on their addresses.
    """
    # Determine run date and followup windows
    run_date = determine_run_date()
    windows = followup_due_date_windows(run_date=run_date, months_ahead=9)
    logger.info(
        "Followup run_date=%s; querying due dates in %s window(s): %s",
        run_date,
        len(windows),
        "; ".join([f"[{w.start}..{(w.end - datetime.timedelta(days=1))}]" for w in windows]),
    )

    # Initialize clients
    dataforsyning_client = DataforsyningClient()
    district_db_client = DistrictMapDBClient()
    cpr_client = CPRClient()

    # Novax session
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

        if not entries:
            logger.info("No matching due dates for this run; exiting.")
            return

        logger.info("Processing %s patients for district/address follow-up", len(entries))

        sentinel_open_end = datetime.datetime(1753, 1, 1)
        sentinel_open_end_date = sentinel_open_end.date()
        now_dt = datetime.datetime.now()
        today = now_dt.date()
        now_time = now_dt.strftime("%H:%M")

        for entry in entries:

            if entry.details is None:
                logger.warning("Skipping Name ID %s: missing NameDetails", entry.ID)
                continue

            # CPR validation and normalization
            normalized_cpr = CPRClient.normalize_cpr_number(entry.CPR)
            if not normalized_cpr:
                logger.warning(
                    "Skipping Name ID %s: invalid CPR value (%s)",
                    entry.ID,
                    CPRClient.mask_cpr_for_log(entry.CPR),
                )
                continue

            # CPR lookup: address UUID + protected status
            cpr_info = cpr_client.get_address_uuid_and_protected_status(normalized_cpr)

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

            if address_info is None:
                logger.warning(
                    "Skipping address + district lookup for Name ID %s due to unexpected Dataforsyning results (adresse_uuid=%s)",
                    entry.ID,
                    cpr_info["address_uuid"],
                )
            else:
                # Address update
                new_full_address = _s(address_info.get("full_address"))
                if new_full_address != _s(entry.ADRESSE):
                    is_new_address_set = True
                    entry.ADRESSE = new_full_address
                    logger.info(f"Updated address for Name ID {entry.ID}")

                    has_valid_address = any(
                        _s(a.NR_LT_ETAGE) == _s(address_info.get("number_floor")) and
                        _i(a.VEJKODE) == address_info.get("street_code") and
                        _s(a.STEDNAVN) == _s(address_info.get("town_name")) and
                        _i(a.POSTNR) == address_info.get("postal_code") and
                        _i(a.KOMMUNEKODE) == address_info.get("municipality_code") and
                        (_to_date(a.DATO_FRA) is not None and _to_date(a.DATO_FRA) <= today) and
                        (
                            _to_date(a.DATO_TIL) is None or
                            _to_date(a.DATO_TIL) == sentinel_open_end_date or
                            _to_date(a.DATO_TIL) > today
                        )
                        for a in entry.addresses
                    )

                    if not has_valid_address:
                        for a in entry.addresses:
                            if _to_date(a.DATO_TIL) is None or _to_date(a.DATO_TIL) == sentinel_open_end_date:
                                a.DATO_TIL = now_dt
                                a.TS_UPDD = now_dt
                                a.TS_UPDT = now_time
                                logger.info(
                                    f"Closed existing address {_s(a.VEJKODE)} {_s(a.NR_LT_ETAGE)} for Name ID {entry.ID} with end date {now_dt.date()}"
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

                # District update
                district = district_db_client.get_district_name_for_point(
                    x=address_info["coordinates"][0],
                    y=address_info["coordinates"][1],
                )

                new_district = _s(district)
                if new_district and new_district != _s(entry.DISTRIKT):
                    is_new_district = True
                    entry.DISTRIKT = new_district
                    logger.info(f"Updated district for Name ID {entry.ID}")

                    if _s(entry.details.TS_KOMID) != new_district:
                        entry.details.TS_KOMID = new_district
                        is_new_district_details = True

                    has_valid_person_district = any(
                        _s(d.DISTRICT) == new_district and
                        (_to_date(d.DATEFROM) is not None and _to_date(d.DATEFROM) <= today) and
                        (
                            _to_date(d.DATETO) is None or
                            _to_date(d.DATETO) == sentinel_open_end_date or
                            _to_date(d.DATETO) > today
                        )
                        for d in entry.person_districts
                    )
                    if not has_valid_person_district:
                        for d in entry.person_districts:
                            if _to_date(d.DATETO) is None or _to_date(d.DATETO) == sentinel_open_end_date:
                                d.DATETO = now_dt
                                d.TS_UPDD = now_dt
                                d.TS_UPDT = now_time
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

            # Always-updates to match main job
            has_changed_active = False
            if _s(entry.AKTIV) in ("", "0"):
                entry.AKTIV = "1"
                has_changed_active = True
                logger.info(f"Updated active status for Name ID {entry.ID}")

            has_changed_ansvarshpl = False
            if entry.AnsvarsShpl != "FIKTIV":
                entry.AnsvarsShpl = "FIKTIV"
                has_changed_ansvarshpl = True
                logger.info(f"Set AnsvarsShpl to 'FIKTIV' for Name ID {entry.ID}")

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
