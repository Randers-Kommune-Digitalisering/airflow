import logging
import pandas as pd

from io import StringIO
from datetime import date, datetime
from requests import Session
from airflow.hooks.base import BaseHook


logger = logging.getLogger(__name__)


class SignflowClient:
    def __init__(self, hook: BaseHook):
        self.url = hook.host.rstrip('/')
        self.session = Session()
        self.username = hook.login
        self.password = hook.password
        self._login()

    def _login(self):
        endpoint = f"{self.url}/usr/auth/basic"
        res = self.session.get(endpoint)
        res.raise_for_status()

        endpoint = f'{self.url}/usr/auth/j_security_check'
        res = self.session.post(endpoint, data={'j_username': self.username, 'j_password': self.password})
        res.raise_for_status()

    def get_authorizations(self) -> pd.DataFrame:
        endpoint = f'{self.url}/usr/ShowDocument'
        params = {'mode': 0, 'FolderStatus_FolderStatusOid': 373, 'sortOrder': 'd', 'sortcolumn': -1, 'pageBeginning': 0, 'csv': 'true'}

        # returns html on first request - ignore response
        res = self.session.get(endpoint, params=params)
        res.raise_for_status()

        # Check if IP is whitelisted
        if "ikke er i listen over godkendte" in res.text:
            raise ValueError("IP is not whitelisted in Signflow")

        # returns csv on second request
        res = self.session.get(endpoint, params=params)
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
