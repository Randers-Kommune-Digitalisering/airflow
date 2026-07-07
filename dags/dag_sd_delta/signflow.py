import logging
import pandas as pd

from io import StringIO
from datetime import date, datetime
from requests import Session
from airflow.hooks.base import BaseHook


logger = logging.getLogger(__name__)


class LogivaSignflowClient:
    """
    Client for interacting with the Logiva Signflow API.

    Args:
        hook (BaseHook): Airflow hook for retrieving connection details.
    """
    def __init__(self, hook: BaseHook):
        self._url = hook.host.rstrip('/')
        self._session = Session()
        self._username = hook.login
        self._password = hook.password
        self._login()

    def _login(self) -> None:
        """
        Perform login to the Signflow API using the provided credentials.
        """
        endpoint = f"{self._url}/usr/auth/basic"
        res = self._session.get(endpoint)
        res.raise_for_status()

        endpoint = f'{self._url}/usr/auth/j_security_check'
        res = self._session.post(endpoint, data={'j_username': self._username, 'j_password': self._password})
        res.raise_for_status()

    def get_authorizations(self) -> pd.DataFrame:
        """
        Fetch authorizations from Signflow and return them as a DataFrame.

        NB: There is no documentation for the Signflow API - this is based on reverse engineering the web interface.

        Returns:
            pd.DataFrame: A DataFrame containing authorizations with columns ['Navn', 'CPR', 'LOS', 'Handling', 'Fra dato', 'Sagsnummer'].
        """
        endpoint = f'{self._url}/usr/ShowDocument'

        params = {'mode': 0, 'FolderStatus_FolderStatusOid': 373, 'sortOrder': 'd', 'sortcolumn': -1, 'pageBeginning': 0, 'csv': 'true'}

        # returns html on first request - ignore response
        res = self._session.get(endpoint, params=params)
        res.raise_for_status()

        # Check if IP is whitelisted
        if "ikke er i listen over godkendte" in res.text:
            raise ValueError("IP is not whitelisted in Signflow")

        # returns csv on second request
        res = self._session.get(endpoint, params=params)
        res.raise_for_status()

        column_names = [
            'Navn', 'CPR', 'Tildelt Login', 'Loginnavn', 'Fra dato', 'LOS', 'Handling',
            'Oprettelsestidspunkt', 'Sagsnummer', 'los1', 'los2', 'los3', 'los4', 'los5',
            'los6', 'los7', 'los8', 'los9', 'lederemail'
        ]
        df = pd.read_csv(
            StringIO(res.content.decode('cp1252')),
            sep='\t',
            names=column_names,
            header=None,
            index_col=False,
            usecols=[0, 1, 4, 5, 6, 8],
            dtype={'CPR': str, 'Fra dato': str},
            on_bad_lines='warn'
        )[['Navn', 'CPR', 'LOS', 'Handling', 'Fra dato', 'Sagsnummer']]

        df = df[df['Handling'].isin(['Genopret', 'Nyansat'])].copy()

        def parse_fra_dato(value: str) -> date | None:
            """
            Parse the 'Fra dato' value into a date object. If the value is NaN or cannot be parsed, return None.

            Args:
                value (str): The 'Fra dato' value to parse.

            Returns:
                date | None: The parsed date object or None if parsing fails.
            """
            if pd.isna(value):
                return None

            text = str(value).strip()
            for date_format in ('%d.%m.%Y', '%d.%m.%y'):
                try:
                    return datetime.strptime(text, date_format).date()
                except ValueError:
                    continue

            return None

        df['Fra dato'] = df['Fra dato'].apply(parse_fra_dato)

        invalid_date_rows = df[df['Fra dato'].isna()]
        for row_index in invalid_date_rows.index:
            logger.warning(
                "Dropping Signflow row due to invalid Fra dato format at index=%s",
                row_index,
            )

        df = df[df['Fra dato'].notna()].copy()

        return df
