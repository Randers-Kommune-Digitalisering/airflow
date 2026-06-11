import io
import logging
import pandas as pd
from typing import Any, Iterable
from openpyxl.utils import get_column_letter
from airflow.models import Variable
from rkdigi.email_handling import EmailReader

logger = logging.getLogger(__name__)

# the excluded_ydelse_name list will from now on be maintained in Airflow Variables, so it can be updated without code changes
excluded_cfg = Variable.get(
    "modregning_excluded_ydelse_list",
    default_var='{"excluded_ydelse_name": []}',
    deserialize_json=True,
)

excluded_ydelse_name = {
    x.strip()
    for x in excluded_cfg.get("excluded_ydelse_name", [])
    if x and isinstance(x, str)
}


def _normalize_cpr(value: str | None) -> str | None:
    """
    Normalize CPR to 10 digits (string). Returns None if invalid.

    :param value: CPR value to normalize.
    :return: Normalized CPR string or None if invalid.
    """
    if value is None:
        return None

    s = value.strip().replace("-", "").replace(" ", "")
    if not s.isdigit():
        return None

    s = s.zfill(10)

    if len(s) != 10:
        return None

    return s


def extract_unique_cprs(
    df: pd.DataFrame,
    column: str = "ID-nummer",
) -> list[str]:
    """
    Extract unique CPR numbers from an Excel DataFrame column.

    :param df: DataFrame from Excel.
    :param column: Column name containing CPR/ID numbers.
    :return: Sorted list of unique CPR strings (10 digits).
    """
    if column not in df.columns:
        raise ValueError(f'Expected column "{column}" not found. Columns: {df.columns.tolist()}')

    normalized = df[column].map(_normalize_cpr)
    unique = sorted(str(cpr) for cpr in normalized.dropna().unique())
    logger.info(f"Extracted {len(unique)} unique CPRs from Excel")
    return unique


def extract_ydelser_from_serviceplatform_response(payload: dict[str, Any]) -> tuple[set[str], bool]:
    """
    Extract unique `YdelseNavn` values from a Serviceplatform response payload.

    :param payload: Parsed JSON/dict response from Serviceplatform.
    :return: (ydelser, found_any_ydelse)
    """
    ydelser: set[str] = set()
    found_any_ydelse = False

    if not isinstance(payload, dict):
        return ydelser, found_any_ydelse

    effektuering = payload.get("EffektueringHent_O", {}) or {}
    oek_list = effektuering.get("OEkonomiskEffektueringListe", []) or []

    for oek in oek_list:
        ydelse_list = oek.get("OEkonomiskYdelseseffektueringListe", []) or []

        for ydelse in ydelse_list:
            found_any_ydelse = True

            navn = ydelse.get("BevilgetYdelse", {}).get("YdelseNavn", "")
            if not isinstance(navn, str):
                continue

            navn = navn.strip()
            if not navn:
                continue

            if navn in excluded_ydelse_name:
                continue

            ydelser.add(navn)

    return ydelser, found_any_ydelse


def df_to_excel_bytes(df: pd.DataFrame, sheet_name: str = "Modregning") -> bytes:
    """
    Convert a DataFrame to an in-memory Excel file.

    :param df: DataFrame to write.
    :param sheet_name: Excel sheet name.
    :return: Excel file as bytes.
    """
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)

        ws = writer.sheets[sheet_name]

        padding = 2
        max_width = 60

        for col_idx, col_name in enumerate(df.columns, start=1):
            series = df[col_name].fillna("").astype(str)

            if series.empty:
                max_len = 0
            else:
                max_len_raw = series.map(len).max()
                max_len = int(max_len_raw) if pd.notna(max_len_raw) else 0

            max_len = max(max_len, len(str(col_name)))
            ws.column_dimensions[get_column_letter(col_idx)].width = min(max_len + padding, max_width)

    return output.getvalue()


def find_latest_modregning_excel_attachment(
    email_reader: EmailReader,
    mailbox: str = "INBOX",
    criteria: str = "UNSEEN",
    filename_prefixes: Iterable[str] = ("Modregning"),
    max_emails: int = 50,
) -> tuple[bytes, str, bytes] | None:
    """
    Find the newest matching Excel attachment in an IMAP mailbox.

    :param email_reader: EmailReader used to fetch emails.
    :param mailbox: Mailbox/folder to search in (e.g. "INBOX").
    :param criteria: IMAP search criteria (e.g. "ALL", "UNSEEN").
    :param filename_prefixes: Allowed attachment filename prefixes.
    :param max_emails: Maximum number of emails to fetch and scan.
    :return: (uid, filename, content_bytes) for the first matching attachment, or None.
    """
    emails, failed = email_reader.get_emails(
        mailbox=mailbox,
        criteria=criteria,
        set_flags=None,
        max=max_emails,
        low_to_high=False,  # start with newest emails first
    )

    logger.info(f"Fetched {len(emails)} email(s), {len(failed)} failed to fetch.")

    for msg in emails:
        uid: bytes = getattr(msg, "uid", None)
        subject = msg.get("Subject", "")
        logger.info(f"Email UID: {uid}, Subject: {subject}")

        for part in msg.iter_attachments():
            filename = part.get_filename() or ""
            filename_lc = filename.lower()
            if not filename_lc.endswith(".xlsx"):
                continue

            if not any(filename_lc.startswith(p.lower()) for p in filename_prefixes):
                continue

            content = part.get_payload(decode=True)  # bytes
            if not content:
                continue

            return uid, filename, content

    return None
