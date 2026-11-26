import logging

from datetime import date, datetime
from io import StringIO

import pandas as pd

from requests import Session
from airflow.hooks.base import BaseHook


logger = logging.getLogger(__name__)


class LogivaSignflowClient:
    """Fetch authorizations for the IT support report from Signflow."""

    def __init__(self, hook: BaseHook):
        self._url = hook.host.rstrip("/")
        self._session = Session()
        self._username = hook.login
        self._password = hook.password
        self._login()

    def _login(self) -> None:
        res = self._session.get(f"{self._url}/usr/auth/basic")
        res.raise_for_status()

        res = self._session.post(
            f"{self._url}/usr/auth/j_security_check",
            data={"j_username": self._username, "j_password": self._password},
        )
        res.raise_for_status()

    def get_authorizations(self) -> pd.DataFrame:
        endpoint = f"{self._url}/usr/ShowDocument"
        params = {
            "mode": 0, "FolderStatus_FolderStatusOid": 373, "sortOrder": "d",
            "sortcolumn": -1, "pageBeginning": 0, "csv": "true",
        }

        # Signflow returns HTML on the first request and CSV on the second.
        res = self._session.get(endpoint, params=params)
        res.raise_for_status()
        if "ikke er i listen over godkendte" in res.text:
            raise ValueError("IP is not whitelisted in Signflow")

        res = self._session.get(endpoint, params=params)
        res.raise_for_status()

        column_names = [
            "Navn", "CPR", "Tildelt Login", "Loginnavn", "Fra dato", "LOS", "Handling",
            "Oprettelsestidspunkt", "Sagsnummer", "los1", "los2", "los3", "los4", "los5",
            "los6", "los7", "los8", "los9", "lederemail",
        ]
        df = pd.read_csv(
            StringIO(res.content.decode("cp1252")),
            sep="\t",
            names=column_names,
            header=None,
            index_col=False,
            usecols=[0, 1, 4, 5, 6, 8, 18],
            dtype={"CPR": str, "Fra dato": str},
            on_bad_lines="warn",
        )[["Navn", "CPR", "LOS", "Handling", "Fra dato", "Sagsnummer", "lederemail"]]

        df = df[df["Handling"].isin(["Modtag", "Genopret", "Nyansat"])].copy()

        def parse_fra_dato(value: str) -> date | None:
            if pd.isna(value):
                return None
            text = str(value).strip()
            for date_format in ("%d.%m.%Y", "%d.%m.%y"):
                try:
                    return datetime.strptime(text, date_format).date()
                except ValueError:
                    continue
            return None

        df["Fra dato"] = df["Fra dato"].apply(parse_fra_dato)
        invalid_date_rows = df[df["Fra dato"].isna()]
        for row_index in invalid_date_rows.index:
            logger.warning("Dropping Signflow row due to invalid Fra dato format at index=%s", row_index)

        return df[df["Fra dato"].notna()].copy()
