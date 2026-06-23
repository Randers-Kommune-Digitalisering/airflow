import logging

from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import and_, func
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

from dag_novax_district_control.clients.cpr_client import CPRClient
from dag_novax_district_control.clients.district_map_client import DistrictMapDBClient
from dag_novax_district_control.clients.dataforsyning_client import DataforsyningClient
from dag_novax_district_control.novax_utils import parse_journal_data, get_allowed_journal_times, normalize_phone_number
from dag_novax_district_control.run_utils import determine_date_range
from dag_novax_district_control.model import Name, Godkommu, Note, PersonUsers, Phone
from dag_novax_district_control.district_update_helpers import (
    clear_district_due_to_missing_address,
    ensure_active,
    is_valid_cpr,
    update_address_from_dataforsyning,
    update_district_from_coordinates,
    update_kommunekode,
    update_protected_address_status,
)

logger = logging.getLogger(__name__)


def check_and_update_district(dry_run: bool, ignore_cprs: list) -> None:
    """
    Retrieves and updates user, address and district information
    for any new patients based on their addresses.
    """
    now_dt = datetime.now()
    now_time = now_dt.strftime("%H:%M")
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
            .join(
                Godkommu,
                Godkommu.NAVNID == Name.ID
            )
            .outerjoin(
                Note,
                and_(
                    Note.NAVNID == Godkommu.NAVNID,
                    Note.DATO == Godkommu.JOURNALDATO,
                    Note.NOTE.like('%>> Orientering - Gravid <<%')
                )
            )
            .filter(
                Godkommu.JOURNALDATO >= start_date,
                Godkommu.JOURNALDATO < end_date,
                Godkommu.EMNEBREV.like('%Orientering - Gravid%'),
                Name.CPR.not_in(ignore_cprs)
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
                matching_phones = [
                    p for p in entry.phones
                    if normalize_phone_number(p.TELEFONNUMMER) == journal_phone
                ]

                existing_primary_phone = next(
                    (p for p in matching_phones if getattr(p, "PRIMAER") == 1),
                    None,
                )

                if not existing_primary_phone:
                    existing_secondary_phone = next(
                        (p for p in matching_phones if getattr(p, "PRIMAER") == 0),
                        None,
                    )

                    for p in entry.phones:
                        if p.PRIMAER == 1:
                            p.PRIMAER = 0
                            p.TS_UPDD = now_dt
                            p.TS_UPDT = now_time

                    if existing_secondary_phone:
                        existing_secondary_phone.PRIMAER = 1
                        existing_secondary_phone.TS_UPDD = now_dt
                        existing_secondary_phone.TS_UPDT = now_time
                        logger.info(
                            f"Updated phone number for Name ID {entry.ID} by setting existing secondary phone as primary"
                        )
                    else:
                        new_phone = Phone(
                            NAVNID=entry.ID,
                            TELEFONNUMMER=journal_phone,
                            PRIMAER=1,
                            TS_DATE=now_dt,
                            TS_TIME=now_time,
                            TS_UPDD=now_dt,
                            TS_UPDT=now_time
                        )
                        entry.phones.append(new_phone)
                        logger.info(f"Added new phone number for Name ID {entry.ID}")

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
                    "Skipping address lookup and clearing district for Name ID %s due to unexpected address information (address_uuid=%s)",
                    entry.ID,
                    address_uuid,
                )

                is_new_district = clear_district_due_to_missing_address(
                    entry=entry,
                    now_dt=now_dt,
                    now_time=now_time,
                )

            else:
                entry_date = entry.date

                is_new_address_set = update_address_from_dataforsyning(
                    entry=entry,
                    address_info=address_info,
                    reference_date=entry_date,
                    close_to_dt=entry_date,
                    new_from_dt=entry_date,
                    now_dt=now_dt,
                    now_time=now_time,
                )

                # District update
                new_district = district_db_client.get_district_name_for_point(
                    x=address_info['coordinates'][0],
                    y=address_info['coordinates'][1],
                )

                is_new_district = update_district_from_coordinates(
                    entry=entry,
                    new_district=new_district,
                    reference_date=entry_date,
                    close_to_dt=entry_date,
                    new_from_dt=entry_date,
                    now_dt=now_dt,
                    now_time=now_time,
                )

                # Kommune update in name and name details
                is_new_kommunekode, is_new_kommunekode_details = update_kommunekode(entry=entry)

            # Always-updates
            # Ensure that the record is active
            has_changed_active = ensure_active(entry=entry)

            # Set AnsvarsShpl to 'FIKTIV' if not already set
            has_changed_ansvarshpl = False
            if entry.AnsvarsShpl != 'FIKTIV':
                entry.AnsvarsShpl = 'FIKTIV'
                has_changed_ansvarshpl = True
                logger.info(f"Set AnsvarsShpl to 'FIKTIV' for Name ID {entry.ID}")

            # Set primary personuser to 'FIKTIV' and demote existing primary personuser
            has_changed_personusers = False
            person_users_rows = entry.person_users

            fiktiv_person_user = next(
                (
                    person_user for person_user in person_users_rows
                    if (person_user.USERID or '').strip() == 'FIKTIV'
                ),
                None,
            )

            for person_user in person_users_rows:
                should_be_primary = fiktiv_person_user is not None and (
                    person_user.RECNUM == fiktiv_person_user.RECNUM
                )
                desired_primary = 1 if should_be_primary else 0

                if person_user.PRIMARY != desired_primary:
                    person_user.PRIMARY = desired_primary
                    person_user.TS_UPDD = now_dt
                    person_user.TS_UPDT = now_time
                    has_changed_personusers = True

            if fiktiv_person_user is None:
                new_person_user = PersonUsers(
                    USERID='FIKTIV',
                    NAVNID=entry.ID,
                    PRIMARY=1,
                    TS_DATE=now_dt,
                    TS_TIME=now_time,
                    TS_UPDD=now_dt,
                    TS_UPDT=now_time,
                )
                # session.add(new_person_user)
                entry.person_users.append(new_person_user)
                has_changed_personusers = True
                logger.info(
                    "Created primary personuser 'FIKTIV' for Name ID %s",
                    entry.ID,
                )
            elif has_changed_personusers:
                logger.info(
                    "Updated personusers to keep only 'FIKTIV' as primary for Name ID %s",
                    entry.ID,
                )

            if any([is_new_district, is_new_address_set, has_changed_active, has_changed_ansvarshpl, has_changed_personusers, is_new_kommunekode]):
                entry.TS_UPDD = now_dt
                entry.TS_UPDT = now_time

            if any([is_due_date_changed, has_changed_protected_status, is_new_district_details, is_new_kommunekode_details]):
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
            logger.info("Successfully completed check_and_update_district")
