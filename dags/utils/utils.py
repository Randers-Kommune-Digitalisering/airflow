import pandas as pd
import io


def df_to_csv_bytes(df: pd.DataFrame, sep: str = ';', encoding: str = 'cp1252') -> io.BytesIO:
    """
    Convert a DataFrame to a CSV file in memory.

    :param df: DataFrame to convert
    :type df: pd.DataFrame
    :param sep: Separator for the CSV file
    :type sep: str
    :param encoding: Encoding for the CSV file
    :type encoding: str
    :return: BytesIO object containing the CSV file
    :rtype: io.BytesIO
    """
    csv_file = io.BytesIO()

    df.to_csv(csv_file, index=False, sep=sep, encoding=encoding)

    csv_file.seek(0)

    return csv_file
