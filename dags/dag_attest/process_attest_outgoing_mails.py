import logging

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import Variable

from dag_attest.model import (
    ForwardStatus,
    ReceivedRecord,
)

from rkdigi.database_manager import DatabaseManager
from rkdigi.email_handling import EmailReader, EmailSender
from utils.mail_messages import (
    build_safe_subject_header,
    get_message_body
)

logger = logging.getLogger(__name__)

_MAILBOX = "INBOX"
_HR_MAILBOX = "personalehr@randers.dk"


def process_attest_outgoing_mails() -> None:
    config = Variable.get("attest_forward_config", deserialize_json=True)
    if not isinstance(config, dict):
        raise AirflowException(
            "Variable 'attest_forward_config' must be a JSON object"
        )
    smtp_server = config.get("smtp_server")
    sender_email = config.get("sender_email")
    if not smtp_server or not sender_email:
        raise AirflowException(
            "Variable 'attest_forward_config' must contain 'smtp_server' "
            "and 'sender_email'"
        )

    imap_conn = BaseHook.get_connection("attest_forward_imap")
    email_reader = EmailReader(
        email=imap_conn.login,
        password=imap_conn.password,
        imap_server=imap_conn.host
    )
    email_sender = EmailSender(smtp_server=smtp_server)

    db_manager = DatabaseManager(
        profile_name="attest_db",
        db_type="postgres",
        airflow_connection_id="attest_db",
    )

    with db_manager.get_session() as session:
        pending = (
            session.query(ReceivedRecord)
            .filter(ReceivedRecord.forward_status == ForwardStatus.PENDING)
            .all()
        )
        items = [
            (
                record.record_id,
                record.mail_uid,
                record.forward_recipient,
                record.order.request.requester_name if record.order else None,
                record.order.request.requester_email if record.order else None
            )
            for record in pending
        ]

    failed_uids = []

    for record_id, mail_uid, recipient, requester_name, requester_email in items:
        try:
            if not recipient:
                raise ValueError("Forwarding recipient is missing")

            message = email_reader.get_email_by_uid(
                uid=mail_uid.encode("ascii"),
                mailbox=_MAILBOX,
            )

            body = get_message_body(message)
            if recipient.casefold() == _HR_MAILBOX:
                body = (
                    "<p>Attesten er ikke ren</p>"
                    f"<p>Rekvirent: {requester_name or 'ukendt'} "
                    f"({requester_email or 'ukendt'})</p>"
                    f"{body}"
                )

            attachments = []
            for part in message.iter_attachments():
                content = part.get_payload(decode=True)
                if content is not None:
                    attachments.append(
                        (part.get_filename() or "attachment", content)
                    )

            email_sender.send_email(
                sender=sender_email,
                recipients=[recipient],
                subject=build_safe_subject_header(
                    raw_subject=str(message.get("Subject") or "")
                ),
                body=body,
                attachments=attachments,
            )

            with db_manager.get_session() as session:
                record = session.get(ReceivedRecord, record_id)
                if record is not None:
                    record.forward_status = ForwardStatus.SENT
                    session.commit()

        except Exception as exc:
            logger.error(
                "Could not forward attest email uid:%r (%s)",
                mail_uid,
                type(exc).__name__
            )
            failed_uids.append(mail_uid)

    if failed_uids:
        raise AirflowException(
            f"Could not forward {len(failed_uids)} attest email(s)"
        )
