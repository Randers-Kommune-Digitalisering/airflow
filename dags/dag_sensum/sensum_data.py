import logging
import fnmatch
import os
import pandas as pd
import tempfile

from datetime import datetime, timedelta
from paramiko import SFTPClient
from typing import Callable, Any
from airflow.providers.sftp.hooks.sftp import SFTPHook
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError

logger = logging.getLogger(__name__)


def fetch_and_store_sensum_data(
    sftp_hook: SFTPHook,
    db_engine: Engine,
    file_patterns: list[str],
    directories: list[str],
    merge_func: Callable,
    output_table: str,
) -> bool:
    """
    Fetch and process Sensum files from SFTP, merge DataFrames, and store in the database.

    :param sftp_hook: Airflow SFTPHook for Sensum SFTP.
    :param db_engine: SQLAlchemy Engine for the database.
    :param file_patterns: List of filename patterns to match.
    :param directories: List of subdirectories on the SFTP server.
    :param merge_func: Function to merge DataFrames.
    :param output_table: Name of the output table in the database.
    :return: True if successful, otherwise False.
    """
    logger.info(f"Processing {output_table}")
    if not (file_patterns and directories):
        raise ValueError("Directories or file patterns are missing")

    file_list_list = []
    with sftp_hook.get_conn() as sftp_conn:
        for pattern in file_patterns:
            files = []
            for directory in directories:
                files += _get_files(
                    sftp_conn=sftp_conn,
                    directory=directory,
                    pattern=pattern,
                )
            if files:
                file_list_list.append(files)
            else:
                raise FileNotFoundError(f"No files found for pattern {pattern}")

        if all(file_list_list):
            return _process_and_save_files(
                file_list_list=file_list_list,
                sftp_conn=sftp_conn,
                merge_func=merge_func,
                db_engine=db_engine,
                output_table=output_table,
            )
    return False


def _get_files(
    sftp_conn: SFTPClient, directory: str, pattern: str, only_latest: bool = False
) -> list[tuple[str, datetime]]:
    """
    Get files from SFTP directory matching pattern, returning filename and modification time.

    :param sftp_conn: Active SFTP connection
    :param directory: Remote directory to scan
    :param pattern: Filename pattern (e.g., '*.csv')
    :param only_latest: If True, return only the latest file
    :return: List of tuples (remote_path, modification_datetime)
    """
    try:
        files = [
            (
                os.path.join(directory, attr.filename),
                datetime.fromtimestamp(attr.st_mtime),
            )
            for attr in sftp_conn.listdir_attr(directory)
            if fnmatch.fnmatch(attr.filename, pattern)
        ]
        if not files:
            logger.info("No files matching pattern found.")
            return []

        # Sort files by mtime descending
        files.sort(key=lambda x: x[1], reverse=True)

        if only_latest:
            return [files[0]]

        return files
    except Exception as e:
        logger.error(f"Error getting files from {directory}: {e}")
        return []


def _download_files_locally(files: list[str], sftp_conn: SFTPClient) -> list[str]:
    """
    Download remote files to local temp files with chunked reading.

    :param files: List of remote file paths
    :param sftp_conn: Active SFTP connection
    :return: List of local file paths
    """
    local_files = []

    for i, remote_path in enumerate(files, start=1):
        local_fd, local_path = tempfile.mkstemp(suffix=os.path.splitext(remote_path)[1])
        logger.debug(
            f"Downloading file {i}/{len(files)}: {remote_path} -> {local_path}"
        )

        fd_closed = False
        try:
            with os.fdopen(local_fd, "wb") as f_local, sftp_conn.open(
                remote_path, "rb"
            ) as f_remote:
                fd_closed = True  # fdopen closes the fd when exiting the context
                while True:
                    chunk = f_remote.read(1024 * 1024)  # 1 MB
                    if not chunk:
                        break
                    f_local.write(chunk)
        except Exception as e:
            logger.error(f"Error downloading {remote_path}: {e}")
            continue
        finally:
            if not fd_closed:
                try:
                    os.close(local_fd)
                except Exception as e:
                    logger.warning(
                        f"Could not close file descriptor for {local_path}: {e}"
                    )

        local_files.append(local_path)

    return local_files


def _handle_files(
    files: list[tuple[str, datetime]], sftp_conn: SFTPClient
) -> pd.DataFrame | None:
    """
    Read and combine relevant Sensum files from SFTP into a single DataFrame.

    :param files: List of tuples (remote_path, modification_datetime)
    :param sftp_conn: Active SFTP connection
    :return: Combined DataFrame or None
    """
    if not files:
        logger.error("No files provided.")
        return None

    latest_file, latest_date = max(files, key=lambda x: x[1])
    logger.info(f"Latest file: {os.path.basename(latest_file)}")

    max_date = latest_date - timedelta(days=1)
    min_date = datetime(latest_date.year - 2, latest_date.month, 1)
    logger.info(f"Data period: {min_date} - {max_date}")

    relevant_files = [f for f, mtime in files if mtime >= min_date]
    logger.info(f"Found {len(relevant_files)} relevant files.")

    if not relevant_files:
        return None

    local_files = _download_files_locally(files=relevant_files, sftp_conn=sftp_conn)

    df_list = []
    try:
        for local_path in local_files:
            df = pd.read_csv(local_path, sep=";", header=0, decimal=",")
            df_list.append(df)
    finally:
        for local_path in local_files:
            try:
                os.remove(local_path)
                logger.debug(f"Removed temp file {local_path}")
            except Exception as e:
                logger.warning(f"Could not remove temp file {local_path}: {e}")

    if df_list:
        combined_df = pd.concat(df_list, ignore_index=True).drop_duplicates()
        return combined_df

    return None


def _process_and_save_files(
    file_list_list: list[list[tuple[str, datetime]]],
    sftp_conn: SFTPClient,
    merge_func: Callable,
    db_engine: Engine,
    output_table: str,
) -> bool:
    """
    Merge and store Sensum data in the database.

    :param file_list_list: List of lists of (remote_path, modification_datetime)
    :param sftp_conn: Paramiko SFTP connection.
    :param merge_func: Function to merge DataFrames.
    :param db_engine: SQLAlchemy Engine.
    :param output_table: Name of the output table.
    :return: True if successful, otherwise False.
    """
    dfs = [
        _handle_files(files=file_list, sftp_conn=sftp_conn)
        for file_list in file_list_list
    ]

    if all(df is not None and not df.empty for df in dfs):
        result = merge_func(*dfs)
        try:
            result.to_sql(
                name=output_table,
                con=db_engine,
                if_exists="replace",
                index=False,
            )
            logger.info(f"Successfully saved {output_table} to the database")
            return True
        except OperationalError as e:
            logger.error(
                f"Operational error while saving {output_table} to the database: {e}"
            )
            return False
        except Exception as e:
            logger.error(f"Failed to save {output_table} to the database: {e}")
            return False

    return False


def merge_dataframes(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    merge_on: list[str],
    group_by: list[str],
    agg_dict: dict[str, Any],
    columns: list[str],
) -> pd.DataFrame:
    """
    Merge two DataFrames, group and aggregate.

    :param df1: First DataFrame.
    :param df2: Second DataFrame.
    :param merge_on: Columns to merge on.
    :param group_by: Columns to group by.
    :param agg_dict: Aggregation dictionary.
    :param columns: Column names for the result.
    :return: Aggregated DataFrame.
    """
    try:
        logger.debug("Merging and aggregating dataframes")
        merged_df = pd.merge(left=df1, right=df2, on=merge_on, how="inner")
        result = merged_df.groupby(group_by).agg(agg_dict).reset_index(drop=True)
        result.columns = columns
        return result
    except Exception as e:
        logger.error(f"Error in merge_dataframes: {e}")
        raise


def merge_df_ydelse(
    ydelse_df: pd.DataFrame,
    afdeling_df: pd.DataFrame,
    group_by: list[str],
    agg_dict: dict[str, Any],
    columns: list[str],
) -> pd.DataFrame:
    """
    Merge ydelse and afdeling DataFrames, group and aggregate.

    :param ydelse_df: Ydelse DataFrame.
    :param afdeling_df: Afdeling DataFrame.
    :param group_by: Columns to group by.
    :param agg_dict: Aggregation dictionary.
    :param columns: Column names for the result.
    :return: Aggregated DataFrame.
    """
    try:
        logger.debug("Merging ydelse and afdeling dataframes")
        ydelse_df = pd.merge(
            left=ydelse_df,
            right=afdeling_df[["AfdelingId", "Navn"]],
            on="AfdelingId",
            how="left",
        )
        result = ydelse_df.groupby(group_by).agg(agg_dict).reset_index(drop=True)
        result.columns = columns
        return result
    except Exception as e:
        logger.error(f"Error in merge_df_ydelse: {e}")
        raise


def sager_afdeling_medarbejder_merge_df(
    sager_df: pd.DataFrame,
    afdeling_df: pd.DataFrame,
    medarbejder_df: pd.DataFrame,
    group_by: list[str],
    agg_dict: dict[str, Any],
    columns: list[str],
) -> pd.DataFrame:
    """
    Merge sager, afdeling, and medarbejder DataFrames, filter and aggregate.

    :param sager_df: Sager DataFrame.
    :param afdeling_df: Afdeling DataFrame.
    :param medarbejder_df: Medarbejder DataFrame.
    :param group_by: Columns to group by.
    :param agg_dict: Aggregation dictionary.
    :param columns: Column names for the result.
    :return: Aggregated DataFrame.
    """
    try:
        logger.debug("Merging sager, afdeling and medarbejder dataframes")
        sager_df = sager_df.rename(columns={"SagModel": "Sager_SagModel"})
        afdeling_df = afdeling_df.rename(
            columns={"Navn": "AfdelingNavn", "AfdelingsId": "AfdelingId"}
        )
        medarbejder_df = medarbejder_df.rename(
            columns={
                "Fornavn": "MedarbejderFornavn",
                "Efternavn": "MedarbejderEfternavn",
            }
        )
        merged_df = pd.merge(
            left=sager_df,
            right=afdeling_df[["AfdelingId", "AfdelingNavn"]],
            on="AfdelingId",
            how="left",
        )
        merged_df = merged_df.rename(columns={"AfdelingNavn_y": "AfdelingNavn"})
        merged_df = pd.merge(
            left=merged_df,
            right=medarbejder_df[
                [
                    "MedarbejderId",
                    "MedarbejderFornavn",
                    "MedarbejderEfternavn",
                    "AfdelingId",
                ]
            ],
            on="AfdelingId",
            how="left",
        )
        merged_df = merged_df[merged_df["Status"] == "Igangværende"]
        result = merged_df.groupby(group_by).agg(agg_dict).reset_index(drop=True)
        result.columns = columns
        return result
    except Exception as e:
        logger.error(f"Error in sager_afdeling_medarbejder_merge_df: {e}")
        raise


def process_indsats_df(
    indsats_df: pd.DataFrame,
    group_by: list[str],
    agg_dict: dict[str, Any],
    columns: list[str],
) -> pd.DataFrame:
    """
    Rename and aggregate indsats DataFrame.

    :param indsats_df: Indsats DataFrame.
    :param group_by: Columns to group by.
    :param agg_dict: Aggregation dictionary.
    :param columns: Column names for the result.
    :return: Aggregated DataFrame.
    """
    try:
        logger.debug("Aggregating indsats dataframe")
        indsats_df = indsats_df.rename(
            columns={
                "StartDato": "IndsatsStartDato",
                "Id": "IndsatsId",
                "Status": "IndsatsStatus",
            }
        )
        result = indsats_df.groupby(group_by).agg(agg_dict).reset_index(drop=True)
        result.columns = columns
        return result
    except Exception as e:
        logger.error(f"Error in process_indsats_df: {e}")
        raise


MERGE_FUNCTIONS: dict[str, Callable] = {
    "merge_dataframes": merge_dataframes,
    "merge_df_ydelse": merge_df_ydelse,
    "sager_afdeling_medarbejder_merge_df": sager_afdeling_medarbejder_merge_df,
    "process_indsats_df": process_indsats_df,
}


def create_merge_lambda(config: dict) -> Callable:
    merge_func = MERGE_FUNCTIONS.get(config["merge_func"])
    if merge_func is None:
        raise ValueError(f"Merge function '{config['merge_func']}' is not allowed.")

    if all(key in config for key in ["merge_on", "group_by", "agg_columns", "columns"]):

        def merge_lambda(*dfs):
            return merge_func(
                *dfs,
                merge_on=config["merge_on"],
                group_by=config["group_by"],
                agg_dict=config["agg_columns"],
                columns=config["columns"],
            )

    elif all(key in config for key in ["group_by", "agg_columns", "columns"]):

        def merge_lambda(*dfs):
            return merge_func(
                *dfs,
                group_by=config["group_by"],
                agg_dict=config["agg_columns"],
                columns=config["columns"],
            )

    else:
        raise Exception("Missing required keys in config")

    return merge_lambda
