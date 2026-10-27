import logging

from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowFailException

from datetime import datetime, timezone

from dag_attest.attest_data import (
    AttestParseError,
    detect_attest_types,
    extract_cpr,
    extract_email_body,
    extract_pdf_attachments,
    find_matching_orders,
    find_received_record,
    is_clean_attest,
    to_attest_type_ids,
)

from dag_attest.model import (
    Notification,
    NotificationType,
    OrderEvent,
    OrderStatus,
    ReceivedRecord,
    RecordOutcome,
)
from rkdigi.email_handling import EmailReader
from rkdigi.database_manager import DatabaseManager

logger = logging.getLogger(__name__)

_MAILBOX = "INBOX"
_ACTOR = "airflow:dag_attest"


def _mark_email_seen(email_reader: EmailReader, uid: bytes | str) -> None:
    uid_text = uid.decode(errors="ignore") if isinstance(uid, bytes) else str(uid)
    marked, _ = email_reader.get_emails(
        mailbox=_MAILBOX,
        criteria=f"UID {uid_text}",
        set_flags="\\Seen",
        max=1,
    )
    if not marked:
        # Safe to retry next run: mail_uid prevents a duplicate ReceivedRecord
        logger.warning("Could not mark attest email uid=%s as seen", uid_text)


def _parse_sbsys_id(value: object, field_name: str) -> int:
    if isinstance(value, bool):
        raise AirflowFailException(
            f"SBSYS response has an invalid {field_name}"
        )

    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise AirflowFailException(
            f"SBSYS response has an invalid {field_name}"
        ) from exc


def _get_sbsys_targets(
        sbsys_client,
        subject_cpr: str,
        attest_types: set[int],
) -> tuple[int, int | None] | None:
    # Børneattest delforløbstitel er endnu ikke verificeret
    if attest_types == {2}:
        return None

    if not attest_types or not attest_types.issubset({0,1}):
        raise AirflowFailException("Unexpected attest type IDs")

    cases = sbsys_client.get_personalesag(subject_cpr)
    if cases is None or not isinstance(cases, list):
        raise AirflowFailException("SBSYS case lookup failed")

    active_cases = []
    for case in cases:
        if not isinstance(case, dict):
            raise AirflowFailException("SBSYS returned an invalid case entry")

        status = case.get("SagsStatus")
        if not isinstance(status, dict) or not isinstance(status.get("Navn"), str):
            raise AirflowFailException("SBSYS case response has no valid status")

        if status["Navn"] == "Aktiv":
            active_cases.append(case)

    if len(active_cases) != 1:
        return None

    sag_id = _parse_sbsys_id(active_cases[0].get("Id"), "case Id")

    subcases = sbsys_client.get_delforloeb(sag_id)
    if subcases is None or not isinstance(subcases, list):
        raise AirflowFailException("SBSYS subcase lookup failed")

    matching_subcases = []
    for subcase in subcases:
        if not isinstance(subcase, dict):
            raise AirflowFailException("SBSYS returned an invalid subcase entry")

        if subcase.get("Titel") == "08 Straffeattester":
            matching_subcases.append(subcase)

    if len(matching_subcases) > 1:
        raise AirflowFailException(
            "SBSYS returned multiple matching attest subcases"
        )

    if not matching_subcases:
        return sag_id, None

    delforloeb_id = _parse_sbsys_id(
        matching_subcases[0].get("ID"),
        "subcase ID",
    )
    return sag_id, delforloeb_id


def process_attest() -> None:
    logger.info("Starting to process attest data...")

    imap_conn = BaseHook.get_connection("attest_imap")
    if not imap_conn.login or not imap_conn.password:
        raise AirflowFailException(
            "Connection 'attest_imap' must include login and password"
        )

    email_reader_kwargs = {
        "email": imap_conn.login,
        "password": imap_conn.password,
    }
    if imap_conn.host:
        email_reader_kwargs["imap_server"] = imap_conn.host
    if imap_conn.port:
        email_reader_kwargs["imap_port"] = int(imap_conn.port)

    email_reader = EmailReader(**email_reader_kwargs)


    # TODO: Create this Airflow Connection with the same ID and correct connection type and details
    db_manager = DatabaseManager(
        profile_name="attest_db",
        db_type="postgres",
        airflow_connection_id="attest_db"
    )
    db_manager.can_connect()

    emails, failed_ids = email_reader.get_emails(
        mailbox=_MAILBOX,
        criteria="UNSEEN",
        set_flags=None
    )

    if failed_ids:
        raise AirflowFailException(
            f"Could not fetch {len(failed_ids)} email(s) from the attest mailbox"
        )

    for message in emails:
        uid = getattr(message, "uid", None)
        if uid is None:
            raise AirflowFailException("Fetched attest email has no UID")
        mail_uid = uid.decode("ascii") if isinstance(uid, bytes) else str(uid)

        try:
            subject_cpr = extract_cpr(extract_email_body(message))

            pdfs = extract_pdf_attachments(message)
            if not pdfs:
                raise AttestParseError("Email has no PDF attachments")

            pdf_bytes, pdf_text = pdfs[0]
            if not pdf_text.strip():
                raise AttestParseError("Could not extract text from PDF")

            attest_types = to_attest_type_ids(
                detect_attest_types(pdf_text, message.get("Subject"))
            )
        except AttestParseError as exc:
            raise AirflowFailException(
                f"Could not parse attest email uid={mail_uid!r}"
            ) from exc

        clean = is_clean_attest(pdf_text)

        with db_manager.get_session() as session:
            existing_record = find_received_record(session, mail_uid)
            if existing_record is not None:
                logger.info(
                    "Attest email uid=%r is already loaded; skipping",
                    mail_uid
                )
            else:
                orders = find_matching_orders(session, subject_cpr, attest_types)

                record = ReceivedRecord(
                    mail_uid=mail_uid,
                    is_clean=clean,
                )
                session.add(record)

                if len(orders) == 1:
                    order = orders[0]
                    order.status = OrderStatus.UNDER_REVIEW
                    record.order = order
                    session.add(
                        OrderEvent(
                            order=order,
                            status=OrderStatus.UNDER_REVIEW,
                            actor=_ACTOR,
                        )
                    )

                session.commit()

                targets = _get_sbsys_targets(
                    sbsys_client,
                    subject_cpr,
                    attest_types
                )
                if targets is None:
                    raise AirflowFailException(
                        f"SBSYS returned no matching targets for email uid={mail_uid!r}"
                    )
                sag_id, delforloeb_id = targets
                
                logger.info(
                    "Loaded attest email uid=%r: %d matching order(s), %s",
                    mail_uid,
                    len(orders),
                    "clean" if clean else "not clean",
                )
        _mark_email_seen(email_reader, uid)
