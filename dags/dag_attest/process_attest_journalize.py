import logging

from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowFailException

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from dag_attest.attest_data import (
    AttestParseError,
    detect_attest_types,
    extract_cpr,
    extract_email_body,
    extract_pdf_attachments,
    extract_requester_email,
    find_matching_orders,
    find_received_record,
    is_clean_attest,
    to_attest_type_ids,
)

from attest_data_model import (
    Notification,
    NotificationType,
    OrderEvent,
    OrderStatus,
    ReceivedRecord,
    RecordOutcome,
    ForwardStatus,
)
from dag_attest.sbsys_client import SbsysClient
from rkdigi.email_handling import EmailReader
from rkdigi.database_manager import DatabaseManager

logger = logging.getLogger(__name__)

_MAILBOX = "INBOX"
_MAIN_POSTBOX = "randers.kommune@randers.dk"
_ACTOR = "airflow:dag_attest"
_DOCUMENT_NAMES = {0: "Offentlig straffeattest", 1: "Privat straffeattest"}


def _queue_forward(session, record, recipient: str) -> None:
    """
    Queue a forward for the given record to the specified recipient.
    
    :param session: SQLAlchemy session
    :param record: The record to forward
    :param recipient: The recipient email address
    """
    record.forward_recipient = recipient
    record.forward_status = ForwardStatus.PENDING
    session.commit()


def _ensure_notification(
    session,
    order_id: str,
    notification_type: NotificationType,
) -> None:
    """
    Ensure that a notification of the specified type exists for the given order.
    
    :param session: SQLAlchemy session
    :param order_id: The ID of the order to check
    :param notification_type: The type of notification to ensure
    """
    existing = (
        session.query(Notification.notification_id)
        .filter(
            Notification.order_id == order_id,
            Notification.notification_type == notification_type,
        )
        .first()
    )

    if existing is None:
        session.add(
            Notification(
                order_id=order_id,
                notification_type=notification_type,
            )
        )


def _mark_email_seen(email_reader: EmailReader, uid: bytes | str) -> None:
    """
    Mark the email with the given UID as seen in the mailbox.
    
    :param email_reader: The EmailReader instance to use for marking the email.
    :param uid: The UID of the email to mark as seen.
    """
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
    """
    Parse and validate an SBSYS ID from the given value.
    
    :param value: The value to parse as an SBSYS ID.
    :param field_name: The name of the field being parsed (for error messages).
    :return: The parsed SBSYS ID as an integer.
    :raises AirflowFailException: If the value is not a valid SBSYS ID.
    """
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
) -> tuple[int, str | None, int | None] | None:
    """
    Get the SBSYS targets for the given subject CPR and attest types.
    
    :param sbsys_client: The SBSYS client to use for API calls.
    :param subject_cpr: The CPR number of the subject.
    :param attest_types: Set of attest type IDs.
    :return: A tuple containing the case ID, case number, and subcase ID, or None if no matching case is found.
    :raises AirflowFailException: If the SBSYS API returns unexpected data or multiple matching subcases are found.
    """
    if attest_types == {2}:
        return None

    if not attest_types or not attest_types.issubset({0, 1}):
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

    sag = active_cases[0]
    sag_id = _parse_sbsys_id(sag.get("Id"), "case Id")
    sag_nummer = sag.get("Nummer")

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
        return sag_id, sag_nummer, None

    delforloeb_id = _parse_sbsys_id(
        matching_subcases[0].get("ID"),
        "subcase ID",
    )
    return sag_id, sag_nummer, delforloeb_id


def _process_email(message, mail_uid: str, db_manager, sbsys_client):
    """
    Process an attest email message.
    
    :param message: The email message to process.
    :param mail_uid: The UID of the email message.
    :param db_manager: The database manager for session handling.
    :param sbsys_client: The SBSYS client for API calls.
    """
    try:
        body = extract_email_body(message)
        subject_cpr = extract_cpr(body)

        pdfs = extract_pdf_attachments(message)
        if not pdfs:
            raise AttestParseError("Email has no PDF attachments")

        # TODO: confirm that you only look at the first PDF attachment and
        # if there are more PDF attachments, they will be ignored for now.
        # The current flow only processes the first PDF attachment.
        pdf_bytes, pdf_text = pdfs[0]
        if not pdf_text.strip():
            raise AttestParseError("Could not extract text from PDF")

        attest_types = to_attest_type_ids(
            detect_attest_types(pdf_text, message.get("Subject"))
        )
    except AttestParseError:
        with db_manager.get_session() as session:
            record = find_received_record(session, mail_uid)
            if record is None:
                record = ReceivedRecord(
                    mail_uid=mail_uid,
                    is_clean=None,
                )
                session.add(record)

            if record.outcome is None and record.forward_status is None:
                _queue_forward(
                    session,
                    record,
                    _MAIN_POSTBOX,
                )

        logger.warning("Attest email uid=%r could not be parsed; queued for manual handling", mail_uid)
        return

    try:
        requester_email = extract_requester_email(body)
    except AttestParseError:
        requester_email = None

    clean = is_clean_attest(pdf_text)

    with db_manager.get_session() as session:
        record = find_received_record(session, mail_uid)
        if record is not None and record.outcome is not None:
            logger.info("Attest email uid=%r has already been processed, skipping", mail_uid)
            return

        orders = find_matching_orders(
            session,
            subject_cpr,
            attest_types,
        )
        if record is None:
            record = ReceivedRecord(
                mail_uid=mail_uid,
                is_clean=clean,
            )
            session.add(record)

        if not orders:
            _queue_forward(
                session,
                record,
                "randers.kommune@randers.dk"
            )
            return

        if len(orders) > 1:
            _queue_forward(
                session,
                record,
                "randers.kommune@randers.dk",
            )
            return

        order = orders[0]
        record.order = order

        if order.status != OrderStatus.UNDER_REVIEW:
            order.status = OrderStatus.UNDER_REVIEW
            session.add(
                OrderEvent(
                    order=order,
                    status=OrderStatus.UNDER_REVIEW,
                    actor=_ACTOR
                )
            )

        if record.sbsys_sags_id is not None:
            raise AirflowFailException(
                f"Attest email uid={mail_uid!r} may already be journalized in SBSYS; check manually"
            )

        if requester_email is None:
            _queue_forward(
                session,
                record,
                _MAIN_POSTBOX
            )
            return

        if (
            record.order is not None
            and record.order.request.requester_email.casefold() != requester_email
        ):
            _queue_forward(
                session,
                record,
                _MAIN_POSTBOX
            )
            return

        targets = _get_sbsys_targets(sbsys_client, subject_cpr, attest_types)
        if targets is None:
            if clean:
                _ensure_notification(
                    session,
                    order.order_id,
                    NotificationType.RECORD_ATTACHED,
                )
            else:
                _ensure_notification(
                    session,
                    order.order_id,
                    NotificationType.RECORD_UNDER_REVIEW
                )
                _ensure_notification(
                    session,
                    order.order_id,
                    NotificationType.RECORD_REQUIRES_REVIEW
                )

            _queue_forward(session, record, _MAIN_POSTBOX)
            return

        sag_id, sag_nummer, delforloeb_id = targets

        record.sbsys_sags_id = sag_id
        session.commit()

        attest_name = _DOCUMENT_NAMES.get(order.attest_type, "Attest")
        today = datetime.now(ZoneInfo("Europe/Copenhagen")).strftime("%d-%m-%Y")
        result = sbsys_client.journalize(
            pdf_bytes=pdf_bytes,
            sag_id=sag_id,
            delforloeb_id=delforloeb_id,
            document_name=f"{attest_name} {today}",
        )

        # TODO: Confirm the SBSYS upload response fields using an anonymized TEST response.
        # A missing/unverified document ID must not cause automatic upload retry;
        # treat HTTP 200 as potentially successful and reconcile manually.
        document_id = result.get("Id") if isinstance(result, dict) else None
        if document_id is None:
            raise AirflowFailException(
                f"SBSYS journalization returned no document id for email uid={mail_uid!r}"
            )

        record.outcome = RecordOutcome.JOURNALIZED
        record.processed_at = datetime.now(timezone.utc)
        record.sbsys_sags_nr = sag_nummer
        record.sbsys_document_id = str(document_id)

        if record.order is not None:
            record.order.status = OrderStatus.JOURNALIZED
            session.add(
                OrderEvent(
                    order=record.order,
                    status=OrderStatus.JOURNALIZED,
                    actor=_ACTOR,
                )
            )

            _ensure_notification(
                session,
                order_id=record.order.order_id,
                notification_type=NotificationType.RECORD_JOURNALIZED
            )

            if not clean:
                _ensure_notification(
                    session,
                    order_id=record.order.order_id,
                    notification_type=NotificationType.RECORD_REQUIRES_REVIEW
                )
        else:
            logger.warning(
                "Attest email uid=%r was journalized without a matcing order; no notification created",
                mail_uid,
            )
        session.commit()

        logger.info("Journalized attest email uid=%r (%s)", mail_uid, "clean" if clean else "not clean")


def process_attest_journalize() -> None:
    """
    Process attest emails by fetching them from the mailbox, handling each email,
    and marking them as seen. Raises an exception if any email could not be processed.
    """
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

    #  TODO: Create this Airflow Connection with the same ID and correct connection type and details
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

    sbsys_client = SbsysClient(BaseHook.get_connection("attest_sbsys"))

    failed: list[str] = []

    for message in emails:
        uid = getattr(message, "uid", None)
        if uid is None:
            logger.error("Fetched attest email has no UID; skipping")
            failed.append("<no uid>")
            continue
        mail_uid = uid.decode("ascii") if isinstance(uid, bytes) else str(uid)

        try:
            _process_email(message, mail_uid, db_manager, sbsys_client)
        except Exception as exc:
            # Only our own messages are logged; others may contain response data
            detail = str(exc) if isinstance(exc, AirflowFailException) else type(exc).__name__
            logger.error("Attest email uid=%r was not processed: %s", mail_uid, detail)
            failed.append(mail_uid)
            continue

        _mark_email_seen(email_reader, uid)

    if failed:
        raise AirflowFailException(
            f"{len(failed)} attest email(s) need manual handling: {', '.join(failed)}"
        )
