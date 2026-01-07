import logging
import fnmatch
import os
import pandas as pd

from datetime import datetime, timedelta
from paramiko import SFTPClient
from typing import List, Callable, Optional, Dict, Any
from airflow.providers.sftp.hooks.sftp import SFTPHook
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError

logger = logging.getLogger(__name__)


def fetch_and_store_sensum_data(
    sftp_hook: SFTPHook,
    db_engine: Engine,
    file_patterns: List[str],
    directories: List[str],
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
    try:
        logger.info(f"Processing {output_table}")
        if not (file_patterns and directories):
            logger.error("Directories or file patterns are missing")
            return False

        file_list_list = []
        with sftp_hook.get_conn() as sftp_conn:
            for pattern in file_patterns:
                files = []
                for subdir in directories:
                    files += _get_files(
                        sftp_conn=sftp_conn,
                        directory="/D:/SFTP-EGDW/",
                        subdirectory=subdir,
                        pattern=pattern,
                    )
                if files:
                    file_list_list.append(files)
                else:
                    logger.error(f"No files found for pattern {pattern}")
                    return False

            if all(file_list_list):
                return _process_and_save_files(
                    file_list_list=file_list_list,
                    sftp_conn=sftp_conn,
                    merge_func=merge_func,
                    db_engine=db_engine,
                    output_table=output_table,
                )
        return False
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return False


def _get_files(
    sftp_conn: SFTPClient,
    directory: str,
    subdirectory: str,
    pattern: str,
    only_latest: bool = False,
) -> List[str]:
    """
    Find files on SFTP matching a pattern, within a directory and subdirectory.

    :param sftp_conn: Paramiko SFTP connection.
    :param directory: Parent directory on SFTP.
    :param subdirectory: Subdirectory on SFTP.
    :param pattern: Filename pattern.
    :param only_latest: If True, only return the latest file.
    :return: List of file paths.
    """
    try:
        full_dir = os.path.join(directory, subdirectory)
        files = [
            os.path.join(full_dir, f)
            for f in sftp_conn.listdir(full_dir)
            if fnmatch.fnmatch(f, pattern)
        ]
        if only_latest and files:
            latest_file = max(files, key=lambda f: sftp_conn.stat(f).st_mtime)
            return [latest_file]
        return files
    except Exception as e:
        logger.error(f"Error getting files: {e}")
        return []


def _handle_files(files: List[str], sftp_conn: SFTPClient) -> Optional[pd.DataFrame]:
    """
    Read and combine relevant Sensum files from SFTP into a single DataFrame.

    :param files: List of file paths.
    :param sftp_conn: Paramiko SFTP connection.
    :return: DataFrame or None.
    """
    try:
        if not files:
            logger.error("No files found.")
            return None

        latest_file = max(files, key=lambda f: sftp_conn.stat(f).st_mtime)
        logger.info(f"Latest file: {os.path.basename(latest_file)}")

        last_modified_time = sftp_conn.stat(latest_file).st_mtime
        date = datetime.fromtimestamp(last_modified_time)

        max_date = date - timedelta(days=1)
        min_date = datetime(date.year - 2, date.month, 1)

        logger.debug(f"Data period: {min_date} - {max_date}")

        files = [
            f
            for f in files
            if datetime.fromtimestamp(sftp_conn.stat(f).st_mtime) >= min_date
        ]

        df_list = []
        for filename in files:
            with sftp_conn.open(filename) as f:
                df = pd.read_csv(f, sep=";", header=0, decimal=",")
                df_list.append(df)
        if df_list:
            df = pd.concat(df_list, ignore_index=True).drop_duplicates()
            return df
        return None
    except Exception as e:
        logger.error(f"Error handling files: {e}")
        return None


def _process_and_save_files(
    file_list_list: List[List[str]],
    sftp_conn: SFTPClient,
    merge_func: Callable,
    db_engine: Engine,
    output_table: str,
) -> bool:
    """
    Merge and store Sensum data in the database.

    :param file_list_list: List of lists of file paths.
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
                name=output_table, con=db_engine, if_exists="replace", index=False
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
    merge_on: List[str],
    group_by: List[str],
    agg_dict: Dict[str, Any],
    columns: List[str],
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
    group_by: List[str],
    agg_dict: Dict[str, Any],
    columns: List[str],
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
    group_by: List[str],
    agg_dict: Dict[str, Any],
    columns: List[str],
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
    group_by: List[str],
    agg_dict: Dict[str, Any],
    columns: List[str],
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


MERGE_FUNCTIONS: Dict[str, Callable] = {
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
