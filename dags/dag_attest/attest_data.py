import logging
import enum
import html
import re

from sqlalchemy.orm import Session
from collections.abc import Collection

from datetime import datetime, timedelta, timezone
from email.message import Message

from dag_attest.model import (
    AttestationOrder,
    AttestationRequest,
    OrderStatus,
    ReceivedRecord,
)

logger = logging.getLogger(__name__)

class AttestParseError(ValueError):
    pass


_CPR_PATTERN = re.compile(r"(?<!\d)\d{6}-?\d{4}(?!\d)")


class AttestType(str, enum.Enum):
    OFFENTLIG_STRAFFEATTEST = "offentlig straffeattest"
    PRIVAT_STRAFFEATTEST = "privat straffeattest"
    BOERNEATTEST = "børneattest"


ATTEST_TYPE_IDS: dict[AttestType, int] = {
    AttestType.OFFENTLIG_STRAFFEATTEST: 0,
    AttestType.PRIVAT_STRAFFEATTEST: 1,
    AttestType.BOERNEATTEST: 2,
}


def extract_pdf_attachments(message: Message) -> list[tuple[bytes, str]]:
    """Return decoded PDF attachment bytes and extracted text."""
    import fitz  # PyMuPDF for PDF processing

    pdfs: list[tuple[bytes, str]] = []

    for part in message.walk():
        if part.get_content_disposition() != "attachment":
            continue

        filename = part.get_filename() or ""
        content_type = part.get_content_type()
        if content_type != "application/pdf" and not filename.casefold().endswith(".pdf"):
            continue

        payload = part.get_payload(decode=True)
        if not payload:
            raise AttestParseError(f"PDF attachment is empty or could not be decoded")

        with fitz.open(stream=payload, filetype="pdf") as document:
            text = "\n".join(page.get_text() for page in document)

        pdfs.append((payload, text))

    return pdfs


def extract_cpr(text: str) -> str:
    lines = re.split(r"<br\s*/?>", text, flags=re.IGNORECASE)
    
    for line in lines:
        plain_line = html.unescape(re.sub(r"<[^>]+>", " ", line))
        if "cpr" not in plain_line.casefold() or ":" not in plain_line:
            continue

        label, value = plain_line.split(":", 1)
        if "cpr" not in label.casefold():
            continue

        value = value.strip()
        if not value:
            break

        # Keep CPR without dash internal
        return value.replace("-", "").strip()

    raise AttestParseError("No CPR field found")


def extract_requester_email(text: str) -> str:
    addresses = {
        match.group(0).casefold()
        for match in re.finditer(
            r"\b[A-Z0-9._%+-]+@randers\.dk\b",
            text,
            flags=re.IGNORECASE
        )
    }

    if len(addresses) != 1:
        raise AttestParseError("Could not uniquely determine requester email")

    return addresses.pop()


def format_cpr_for_sbsys(cpr: str) -> str:
    if "-" in cpr:
        return cpr

    return f"{cpr[:6]}-{cpr[6:]}"


def is_clean_attest(text: str) -> bool:
    normalized_text = " ".join(text.split()).casefold()
    return "er ikke registreret med afgørelser" in normalized_text


def detect_attest_types(text: str, subject: str | None) -> set[AttestType]:
    normalized_text = " ".join(text.split()).casefold()
    found = {kind for kind in AttestType if kind.value in normalized_text}

    if len(found) == 1:
        return found
    if len(found) > 1:
        raise AttestParseError("PDF mentions multiple attest types")

    normalized_subject = (subject or "").casefold()
    if "børneatt" in normalized_subject:
        return {AttestType.BOERNEATTEST}
    if "straffeatt" in normalized_subject:
        return {AttestType.OFFENTLIG_STRAFFEATTEST, AttestType.PRIVAT_STRAFFEATTEST}

    raise AttestParseError("Could not detect attest type from text or subject")


def to_attest_type_ids(kinds: Collection[AttestType]) -> set[int]:
    missing = [kind.value for kind in kinds if kind not in ATTEST_TYPE_IDS]
    if missing:
        raise AttestParseError(f"Unknown attest type value for: {', '.join(missing)}")
    return {ATTEST_TYPE_IDS[kind] for kind in kinds}


def find_matching_orders(
        session: Session,
        subject_cpr: str,
        attest_types: Collection[int],
) -> list[AttestationOrder]:
    """
    Find orders matching CPR, attestType and status 'ordered'
    
    :param session: SQLAlchemy session
    :param subject_cpr: CPR number of the subject
    :param attest_types: Types of attestation
    :return: List of matching AttestationOrder objects
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=1)

    return (
        session.query(AttestationOrder)
        .join(AttestationRequest)
        .filter(
            AttestationRequest.subject_cpr == subject_cpr,
            AttestationRequest.requested_at >= cutoff,
            AttestationOrder.attest_type.in_(attest_types),
            AttestationOrder.status == OrderStatus.ORDERED,
        )
        .all()
    )


def extract_email_body(message: Message) -> str:
    if message.is_multipart():
        body_part = message.get_body(preferencelist=("html", "plain"))
        if body_part is None:
            raise AttestParseError("Email has no readable text body")
    else:
        body_part = message

    payload = body_part.get_payload(decode=True)
    if payload is None:
        raise AttestParseError("Email body could not be decoded")

    charsets = (
        body_part.get_content_charset(),
        "utf-8",
        "cp1252",
        "iso-8859-1",
    )
    tried: set[str] = set()

    for charset in charsets:
        if not charset or charset.lower() in tried:
            continue
        tried.add(charset.lower())
        try:
            return payload.decode(charset)
        except (LookupError, UnicodeDecodeError):
            continue

    raise AttestParseError("Email body has an unsupported character encoding")



def find_received_record(
        session: Session,
        mail_uid: str,
) -> ReceivedRecord | None:
    return (
        session.query(ReceivedRecord)
        .filter(ReceivedRecord.mail_uid == mail_uid)
        .one_or_none()
    )