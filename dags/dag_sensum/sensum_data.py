import logging
import fnmatch
import os
import time
import tempfile
import pandas as pd

from datetime import datetime, timedelta
from paramiko import SFTPClient
from sqlalchemy.engine import Engine


logger = logging.getLogger(__name__)


def get_files(
        sftp_conn: SFTPClient,
        dir: str,
        pattern: str,
        chunk_size: int = 1024 * 1024,
        sleep_time: float = 0.01
) -> list[str]:
    """
    Download files from SFTP server matching a pattern within a date range.
    
    :param sftp_conn: SFTP client connection
    :type sftp_conn: SFTPClient
    :param dir: Directory to search for files
    :type dir: str
    :param pattern: Filename pattern to match
    :type pattern: str
    :param chunk_size: Size of chunks to read from remote file
    :type chunk_size: int
    :param sleep_time: Time to sleep between reading chunks
    :type sleep_time: float
    :return: List of downloaded file paths
    :rtype: list[str]
    """
    files = [
        (
            os.path.join(dir, attr.filename),
            datetime.fromtimestamp(attr.st_mtime),
        )
        for attr in sftp_conn.listdir_attr(dir)
        if fnmatch.fnmatch(attr.filename, pattern)
    ]

    if not files:
        raise FileNotFoundError(f'No files found in directory "{dir}" matching pattern "{pattern}".')

    files.sort(key=lambda x: x[1], reverse=True)

    max_date = datetime.now() - timedelta(days=1)
    min_date = datetime(max_date.year - 2, max_date.month, 1)
    filtered_files = [f for f in files if min_date <= f[1] <= max_date]

    if not filtered_files:
        filtered_files = [files[0]]

    downloaded = []
    for remote_path, _ in filtered_files:
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            with sftp_conn.open(remote_path, "rb") as remote_file:
                while True:
                    chunk = remote_file.read(chunk_size)
                    if not chunk:
                        break
                    tmp_file.write(chunk)
                    tmp_file.flush()
                    time.sleep(sleep_time)
            downloaded.append(tmp_file.name)
    return downloaded


def files_to_postgres(
        db_engine: Engine,
        table_name: str,
        key_col: str,
        cols: list[str],
        file_paths: list[str],
        sec_cols: list[str] | None = None,
        sec_file_paths: list[str] | None = None,
        merge_on: list[str] | None = None,
        filter: list[str] | None = None
) -> None:
    """
    Load data from CSV files into a PostgreSQL table, optionally merging with secondary data and applying filters.

    :param db_engine: SQLAlchemy engine for the target PostgreSQL database
    :type db_engine: Engine
    :param table_name: Name of the target table in the database
    :type table_name: str
    :param key_col: Primary key column name for deduplication
    :type key_col: str
    :param cols: List of columns to load from the primary files
    :type cols: list[str]
    :param file_paths: List of paths to the primary CSV files
    :type file_paths: list[str]
    :param sec_cols: List of columns to load from the secondary files
    :type sec_cols: list[str] | None
    :param sec_file_paths: List of paths to the secondary CSV files
    :type sec_file_paths: list[str] | None
    :param merge_on: List of columns to merge on between primary and secondary data
    :type merge_on: list[str] | None
    :param filter: List containing column name and value to filter the final DataFrame
    :type filter: list[str] | None
    """

    sec_params = [sec_cols, sec_file_paths, merge_on]
    if any(sec_params) and not all(sec_params):
        raise ValueError("sec_cols, sec_file_paths, and merge_on must all be provided together or all be None.")

    sec_combined = None
    if all(sec_params):
        merge_df_cols = sec_cols + [merge_on[0]]
        sec_dfs = []
        for path in sec_file_paths:
            df = pd.read_csv(path, usecols=merge_df_cols, sep=";", header=0, decimal=",")
            sec_dfs.append(df)
        sec_combined = pd.concat(sec_dfs, ignore_index=True)
        sec_combined = sec_combined.drop_duplicates(subset=merge_df_cols, keep="first")

    dfs = []
    usecols = cols + [key_col]
    if sec_cols is not None:
        usecols = usecols + [merge_on[-1]]

    for path in file_paths:
        df = pd.read_csv(path, usecols=usecols, sep=";", header=0, decimal=",")
        dfs.append(df)

    combined = pd.concat(dfs, ignore_index=True)
    combined = combined.drop_duplicates(subset=key_col, keep="first")
    combined = combined.drop(columns=[key_col])

    if sec_combined is not None:
        if len(merge_on) == 2:
            completed_df = pd.merge(combined, sec_combined, how="left", left_on=merge_on[1], right_on=merge_on[0])
        else:
            completed_df = pd.merge(combined, sec_combined, how="left", on=merge_on[0])
        completed_df.drop(columns=merge_on, inplace=True)
    else:
        completed_df = combined

    if filter is not None:
        completed_df = completed_df[completed_df[filter[0]] == filter[1]]
    completed_df.to_sql(table_name, db_engine, if_exists="replace", index=False)
