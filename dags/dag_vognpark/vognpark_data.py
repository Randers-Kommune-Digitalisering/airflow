import io
import pandas as pd
import logging
from utils.config import VOGNPARK_SFTP_DIR

logger = logging.getLogger(__name__)

VOGNPARK_COLUMNS = [
    "Level_1", "Level_2", "Level_3", "Level_4", "Level_5", "Level_6",
    "Art", "Træk", "Drivmiddel", "Reg. nr.", "Mærke", "Model",
    "Anvendelse", "Stel nr. "
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


def get_latest_vognpark_excel_path(sftp_client, directory: str = VOGNPARK_SFTP_DIR) -> str | None:
    """
    Finds the latest Excel file in the given SFTP directory and returns its path.
    """
    sftp = sftp_client.get_conn()
    files = sftp.listdir_attr(directory)
    excel_files = [f for f in files if f.filename.lower().endswith(".xlsx")]

    if not excel_files:
        logger.error(f"No Excel files found in SFTP directory: {directory}")
        return None

    latest = max(excel_files, key=lambda f: f.st_mtime)
    return directory.rstrip("/") + "/" + latest.filename
