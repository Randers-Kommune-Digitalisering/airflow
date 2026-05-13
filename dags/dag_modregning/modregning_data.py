import fnmatch
import io
import logging
import pandas as pd
from datetime import datetime, timezone
from typing import Any
from openpyxl.utils import get_column_letter
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.models import Variable

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


def get_latest_excel_info(
    sftp_hook: SFTPHook,
    directory: str,
    pattern: str = "*.xlsx",
) -> tuple[str, datetime] | None:
    """
    Find the newest Excel file on an SFTP matching a filename pattern.

    :param sftp_hook: Airflow SFTPHook for SFTP.
    :param directory: Remote directory to search in.
    :param pattern: Glob pattern for matching filenames (default '*.xlsx').
    :return: (remote_path, modified_at_utc) if a match is found, otherwise None.
    """

    sftp = sftp_hook.get_conn()
    files = sftp.listdir_attr(directory)

    matched = [f for f in files if fnmatch.fnmatch(f.filename.lower(), pattern.lower())]
    if not matched:
        logger.error(f'No Excel files found in "{directory}" matching "{pattern.lower()}"')
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
    """
    Read an Excel file from SFTP into a pandas DataFrame.

    :param sftp_hook: Airflow SFTPHook used to obtain an SFTP connection.
    :param remote_path: Full remote path to the Excel file to read.
    :param dtype: Optional dtype mapping forwarded to `pandas.read_excel`.
    :param sheet_name: Sheet name or index to read, forwarded to `pandas.read_excel` (default: 0, i.e. first sheet).
    :return: DataFrame containing the parsed Excel sheet.
    """

    logger.info(f"Reading Excel file from SFTP: {remote_path}")

    sftp = sftp_hook.get_conn()
    with sftp.open(remote_path, "rb") as remote_file:
        data = remote_file.read()

    return pd.read_excel(io.BytesIO(data), dtype=dtype, sheet_name=sheet_name)


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
    # `dropna()` removes None/NaN, then `unique()` is faster than `set(...)` for large Series.
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
