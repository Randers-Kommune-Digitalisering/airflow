import logging

from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import and_, func
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

from dag_novax_district_control.clients.cpr_client import CPRClient
from dag_novax_district_control.clients.district_map_client import DistrictMapDBClient
from dag_novax_district_control.clients.dataforsyning_client import DataforsyningClient
from dag_novax_district_control.novax_utils import parse_journal_data, get_allowed_journal_times
from dag_novax_district_control.run_utils import determine_date_range
from dag_novax_district_control.model import Name, Godkommu, Note, Phone, PersonDistrict, Address

logger = logging.getLogger(__name__)


def check_and_update_district(dry_run: bool) -> None:
    """
    Retrieves and updates user, address and district information
    for any new patients based on their addresses.
    """
    # Determine date range for processing
    # Start date is inclusive, end date is exclusive
    start_date, end_date = determine_date_range()

    # Initialize clients
    dataforsyning_client = DataforsyningClient()
    district_db_client = DistrictMapDBClient()
    cpr_client = CPRClient()

    # Novax session
    hook = MsSqlHook(mssql_conn_id="novax_sql")
    engine = hook.get_sqlalchemy_engine()

    with Session(engine) as session:
        results = (
            session.query(Name, Godkommu, Note)
            .join(Godkommu, Godkommu.NAVNID == Name.ID)
            .outerjoin(
                Note,
                and_(
                    Note.NAVNID == Godkommu.NAVNID,
                    Note.DATO == Godkommu.JOURNALDATO,
                    Note.NOTE.like('%Orientering - Gravid%')
                )
            )
            .filter(
                Godkommu.JOURNALDATO >= start_date,
                Godkommu.JOURNALDATO <= end_date,
                Godkommu.EMNEBREV.like('%Orientering - Gravid%')
            )
            .order_by(Godkommu.NAVNID, Godkommu.JOURNALDATO.desc(), func.trim(Godkommu.JOURNALTID).desc())
            .all()
        )

        # Set journal to Name objects
        entries = []
        seen_navnids = set()
        for name_obj, godkommu_obj, note_obj in results:
            navnid = godkommu_obj.NAVNID
            if navnid in seen_navnids:
                continue
            assigned = False
            if note_obj and godkommu_obj.JOURNALTID:
                allowed_times = get_allowed_journal_times(godkommu_obj.JOURNALTID)
                note_time = note_obj.TIDSPUNKT.strip()
                if note_time in allowed_times:
                    name_obj.date = note_obj.DATO
                    name_obj.journal = parse_journal_data(note_obj.NOTE)
                    assigned = True
                    entries.append(name_obj)
                    seen_navnids.add(navnid)

            if not assigned:
                logger.warning(f"No pregnancy note found for Name ID {name_obj.ID} with journal timestamp {godkommu_obj.JOURNALTID}. Skipping entry.")

        logger.info(f"Processing {len(entries)} entries for date range {start_date} to {end_date}")

        sentinel_open_end = datetime(1753, 1, 1)
        sentinel_open_end_date = sentinel_open_end.date()

        for entry in entries:
            # CPR validation
            if not (entry.CPR.isdigit() and len(entry.CPR) == 10):
                logger.warning(
                    "Skipping Name ID %s: invalid CPR value",
                    entry.ID
                )
                continue

            # Due date
            current_due_date = entry.details.TERMIN.date()
            journal_due_date = entry.journal.get('due_date')
            calculated_due_date = entry.journal.get('calculated_due_date')
            new_due_date = None
            if journal_due_date and journal_due_date != current_due_date:
                new_due_date = journal_due_date
            elif calculated_due_date and calculated_due_date != current_due_date:
                new_due_date = calculated_due_date

            is_due_date_changed = False
            if new_due_date:
                entry.details.TERMIN = new_due_date
                is_due_date_changed = True
                logger.info(f"Updated due date for Name ID {entry.ID} from {current_due_date} to {new_due_date}")

            # Phone number update
            journal_phone = entry.journal.get('phone')
            if journal_phone:
                is_phone_already_set = any(
                    p.TELEFONNUMMER == journal_phone and getattr(p, "PRIMAER", 0) == 1
                    for p in entry.phones
                )
                if not is_phone_already_set:
                    secondary_phone = next(
                        (p for p in entry.phones if p.TELEFONNUMMER == journal_phone and getattr(p, "PRIMAER", 0) == 0),
                        None
                    )
                    if secondary_phone:
                        for p in entry.phones:
                            if p.PRIMAER == 1:
                                p.PRIMAER = 0
                                p.TS_UPDD = datetime.now()
                                p.TS_UPDT = datetime.now().strftime("%H:%M")
                        secondary_phone.PRIMAER = 1
                        secondary_phone.TS_UPDD = datetime.now()
                        secondary_phone.TS_UPDT = datetime.now().strftime("%H:%M")
                        logger.info(f"Updated phone number for Name ID {entry.ID} to {journal_phone} by setting existing secondary phone as primary")
                    else:
                        for p in entry.phones:
                            if p.PRIMAER == 1:
                                p.PRIMAER = 0
                                p.TS_UPDD = datetime.now()
                                p.TS_UPDT = datetime.now().strftime("%H:%M")
                        new_phone = Phone(
                            NAVNID=entry.ID,
                            TELEFONNUMMER=journal_phone,
                            PRIMAER=1,
                            TS_DATE=datetime.now(),
                            TS_TIME=datetime.now().strftime("%H:%M"),
                            TS_UPDD=datetime.now(),
                            TS_UPDT=datetime.now().strftime("%H:%M")
                        )
                        entry.phones.append(new_phone)
                        logger.info(f"Added new phone number for Name ID {entry.ID}: {journal_phone}")

            # CPR lookup: address UUID + protected status
            cpr_info = cpr_client.get_address_uuid_and_protected_status(entry.CPR)

            has_changed_protected_status = False
            if cpr_info['is_protected_address'] != bool(entry.details.BESKYTTETADRESSE):  # small int value in DB
                entry.details.BESKYTTETADRESSE = int(cpr_info['is_protected_address'])
                has_changed_protected_status = True
                logger.info(f"Updated protected address status for Name ID {entry.ID} to {cpr_info['is_protected_address']}")

            address_info = dataforsyning_client.get_address_by_id(cpr_info['address_uuid'])

            # Address + district updates
            is_new_address_set = None
            is_new_district = None
            is_new_district_details = False

            if address_info is None:
                logger.warning(
                    "Skipping address + district lookup for Name ID %s due to unexpected Dataforsyning results (adresse_uuid=%s)",
                    entry.ID,
                    cpr_info['address_uuid'],
                )
            else:
                entry_date = entry.date

                # Address update
                new_full_address = address_info.get('full_address')
                if new_full_address != entry.ADRESSE:
                    is_new_address_set = True
                    entry.ADRESSE = new_full_address
                    logger.info(f"Updated address for Name ID {entry.ID}")
                    has_valid_address = any(
                        a.NR_LT_ETAGE == address_info.get('number_floor') and
                        a.VEJKODE == address_info.get('street_code') and
                        a.STEDNAVN == address_info.get('town_name') and
                        a.POSTNR == address_info.get('postal_code') and
                        a.KOMMUNEKODE == address_info.get('municipality_code') and
                        (entry_date is not None and a.DATO_FRA is not None and a.DATO_FRA <= entry_date) and
                        (
                            a.DATO_TIL is None or
                            a.DATO_TIL == sentinel_open_end_date or
                            (entry_date is not None and a.DATO_TIL > entry_date)
                        )
                        for a in entry.addresses
                    )

                    if not has_valid_address:
                        for a in entry.addresses:
                            if a.DATO_TIL is None or a.DATO_TIL == sentinel_open_end_date:
                                a.DATO_TIL = entry.date
                                a.TS_UPDD = datetime.now()
                                a.TS_UPDT = datetime.now().strftime("%H:%M")
                                logger.info(
                                    f"Closed existing address {a.VEJKODE} {a.NR_LT_ETAGE} for Name ID {entry.ID} with end date {entry.date}"
                                )
                        new_address_entry = Address(
                            NAVNID=entry.ID,
                            VEJKODE=str(address_info['street_code']),
                            KOMMUNEKODE=str(address_info['municipality_code']),
                            POSTNR=str(address_info['postal_code']),
                            STEDNAVN=address_info['town_name'],
                            NR_LT_ETAGE=address_info['number_floor'],
                            DATO_FRA=entry.date,
                            DATO_TIL=sentinel_open_end,
                            TS_DATE=datetime.now(),
                            TS_TIME=datetime.now().strftime("%H:%M"),
                            TS_UPDD=datetime.now(),
                            TS_UPDT=datetime.now().strftime("%H:%M")
                        )
                        entry.addresses.append(new_address_entry)
                        logger.info(f"Added new address for Name ID {entry.ID}")

                # District update
                district = district_db_client.get_district_name_for_point(
                    x=address_info['coordinates'][0],
                    y=address_info['coordinates'][1],
                )
                new_district = district
                if new_district and new_district != entry.DISTRIKT:
                    is_new_district = True
                    entry.DISTRIKT = new_district
                    current_ts_komid = entry.details.TS_KOMID
                    if current_ts_komid != new_district:
                        entry.details.TS_KOMID = new_district
                        is_new_district_details = True
                        logger.info(f"Updated district details for Name ID {entry.ID} to {new_district}")
                    logger.info(f"Updated district for Name ID {entry.ID} to {new_district}")

                    has_valid_person_district = any(
                        d.DISTRICT == new_district and
                        (entry_date is not None and d.DATEFROM is not None and d.DATEFROM <= entry_date) and
                        (
                            d.DATETO is None or
                            d.DATETO == sentinel_open_end_date or
                            (entry_date is not None and d.DATETO > entry_date)
                        )
                        for d in entry.person_districts
                    )
                    if not has_valid_person_district:
                        for d in entry.person_districts:
                            if d.DATETO is None or d.DATETO == sentinel_open_end_date:
                                d.DATETO = entry.date
                                d.TS_UPDD = datetime.now()
                                d.TS_UPDT = datetime.now().strftime("%H:%M")
                                logger.info(f"Closed existing person district {d.DISTRICT} for Name ID {entry.ID} with end date {entry.date}")
                        new_person_district = PersonDistrict(
                            NAVNID=entry.ID,
                            DISTRICT=new_district,
                            DATEFROM=entry.date,
                            DATETO=sentinel_open_end,
                            TS_DATE=datetime.now(),
                            TS_TIME=datetime.now().strftime("%H:%M"),
                            TS_UPDD=datetime.now(),
                            TS_UPDT=datetime.now().strftime("%H:%M")
                        )
                        entry.person_districts.append(new_person_district)
                        logger.info(f"Added new person district for Name ID {entry.ID}: {new_district}")

            # Always-updates
            has_changed_active = False
            if entry.AKTIV in ("", "0"):
                entry.AKTIV = "1"
                has_changed_active = True
                logger.info(f"Set AKTIV to 1 for Name ID {entry.ID}")

            has_changed_ansvarshpl = False
            if entry.AnsvarsShpl != 'FIKTIV':
                entry.AnsvarsShpl = 'FIKTIV'
                has_changed_ansvarshpl = True
                logger.info(f"Set AnsvarsShpl to 'FIKTIV' for Name ID {entry.ID}")

            if any([is_new_district, is_new_address_set, has_changed_active, has_changed_ansvarshpl]):
                entry.TS_UPDD = datetime.now()
                entry.TS_UPDT = datetime.now().strftime("%H:%M")

            if any([is_due_date_changed, has_changed_protected_status, is_new_district_details]):
                entry.details.TS_UPDD = datetime.now()
                entry.details.TS_UPDT = datetime.now().strftime("%H:%M")

        if dry_run:
            logger.warning("Dry run enabled - no changes committed to the database")
        else:
            logger.info("Committing changes to the database")
            session.commit()
