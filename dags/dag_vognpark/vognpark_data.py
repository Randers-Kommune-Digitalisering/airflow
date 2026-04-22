import io
import pandas as pd
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


VOGNPARK_COLUMNS = [
    "Level1", "Level2", "Level3", "Level4", "Level5", "Level6",
    "Art", "Træk", "Drivmiddel", "Reg.nr.", "Mærke", "Model",
    "Anvendelse", "Stelnr.", "Afg.dato"
]


def read_vognpark_excel_from_sftp(sftp_client, remote_path: str) -> pd.DataFrame:
    """
    Reads Vognpark Excel file from SFTP and returns a DataFrame with selected columns.
    """
    logger.debug(f"Reading Excel file from SFTP: {remote_path}")
    sftp = sftp_client.get_conn()
    with sftp.open(remote_path, 'rb') as remote_file:
        data = remote_file.read()

    df = pd.read_excel(io.BytesIO(data))
    missing_cols = [col for col in VOGNPARK_COLUMNS if col not in df.columns]
    if missing_cols:
        logger.error(f"Missing columns in Excel file: {missing_cols}")

    df = df[VOGNPARK_COLUMNS]
    logger.debug(f"Read {len(df)} rows from Excel file")
    return df


def get_latest_vognpark_excel_info(
    sftp_client,
    directory: str = "/Vognpark/",
) -> tuple[str, datetime] | None:
    """
    Returns (latest_file_path, modified_at_utc) for the newest .xlsx in directory.
    modified_at is based on SFTP st_mtime.
    """
    sftp = sftp_client.get_conn()
    files = sftp.listdir_attr(directory)
    excel_files = [f for f in files if f.filename.lower().endswith(".xlsx")]

    if not excel_files:
        logger.error(f"No Excel files found in SFTP directory: {directory}")
        return None

    latest = max(excel_files, key=lambda f: f.st_mtime)
    latest_path = directory.rstrip("/") + "/" + latest.filename
    modified_at_utc = datetime.fromtimestamp(latest.st_mtime, tz=timezone.utc)

    return latest_path, modified_at_utc
