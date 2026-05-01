import fnmatch
import io
import logging
from datetime import datetime, timezone
from typing import Any

import pandas as pd
from airflow.providers.sftp.hooks.sftp import SFTPHook

logger = logging.getLogger(__name__)

DEFAULT_CPR_COLUMN = "ID-nummer"


def get_latest_excel_info(
    sftp_hook: SFTPHook,
    directory: str,
    pattern: str = "*.xlsx",
) -> tuple[str, datetime] | None:
    sftp = sftp_hook.get_conn()
    files = sftp.listdir_attr(directory)

    matched = [f for f in files if fnmatch.fnmatch(f.filename.lower(), pattern.lower())]
    if not matched:
        logger.error(f'No Excel files found in "{directory}" matching "{pattern}"')
        return None

    latest = max(matched, key=lambda f: f.st_mtime)
    remote_path = directory.rstrip("/") + "/" + latest.filename
    modified_at_utc = datetime.fromtimestamp(latest.st_mtime, tz=timezone.utc)
    return remote_path, modified_at_utc


def read_excel_from_sftp(
    sftp_hook: SFTPHook,
    remote_path: str,
    dtype: dict[str, Any] | None = None,
    sheet_name: str | int | None = 0,
) -> pd.DataFrame:
    logger.info(f"Reading Excel file from SFTP: {remote_path}")

    sftp = sftp_hook.get_conn()
    with sftp.open(remote_path, "rb") as remote_file:
        data = remote_file.read()

    return pd.read_excel(io.BytesIO(data), dtype=dtype, sheet_name=sheet_name)


def mask_cpr(cpr: object) -> str:
    """
    Mask CPR for logging (never log full CPR).

    :param cpr: CPR-like value.
    :return: Masked CPR like 'DDMMYYxxxx' or 'invalid'.
    """
    cpr_str = str(cpr).strip().replace("-", "").replace(" ", "").zfill(10)
    if len(cpr_str) == 10 and cpr_str.isdigit():
        return f"{cpr_str[:6]}xxxx"
    return "invalid"


def _normalize_cpr(value: object) -> str | None:
    """
    Normalize CPR to 10 digits (string). Returns None if invalid.

    :param value: Raw CPR cell value from Excel.
    :return: 10-digit CPR string or None.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None

    s = str(value).strip().replace("-", "").replace(" ", "")
    if not s.isdigit():
        return None

    s = s.zfill(10)
    return s if len(s) == 10 else None


def extract_unique_cprs(
    df: pd.DataFrame,
    column: str = DEFAULT_CPR_COLUMN,
) -> list[str]:
    """
    Extract unique CPR numbers from an Excel DataFrame column.

    :param df: DataFrame from Excel.
    :param column: Column name containing CPR/ID numbers.
    :return: Sorted list of unique CPR strings (10 digits).
    :raises ValueError: If expected column is missing.
    """
    if column not in df.columns:
        raise ValueError(f'Expected column "{column}" not found. Columns: {df.columns.tolist()}')

    cprs: list[str] = []
    for raw in df[column].tolist():
        normalized = _normalize_cpr(raw)
        if normalized:
            cprs.append(normalized)

    unique = sorted(set(cprs))
    logger.info(f"Extracted {len(unique)} unique CPRs from Excel")
    return unique


def extract_ydelser_from_serviceplatform_response(payload: dict[str, Any]) -> set[str]:
    """
    Extract unique YdelseNavn values from the Serviceplatform payload (new format).

    :param payload: JSON payload returned by the Serviceplatform endpoint.
    :return: Set of unique YdelseNavn strings.
    """
    ydelser: set[str] = set()

    if not isinstance(payload, dict):
        return ydelser

    effektuering = payload.get("EffektueringHent_O", {}) or {}
    oek_list = effektuering.get("OEkonomiskEffektueringListe", []) or []

    for oek in oek_list:
        # New structure: list is directly under each item
        ydelse_list = oek.get("OEkonomiskYdelseseffektueringListe", []) or []

        for ydelse in ydelse_list:
            navn = (ydelse.get("BevilgetYdelse", {}) or {}).get("YdelseNavn", "")
            if isinstance(navn, str):
                navn = navn.strip()
                if navn:
                    ydelser.add(navn)

    return ydelser


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
    return output.getvalue()
