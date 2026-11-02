import json
import logging
from urllib import response

import requests

from dag_attest.attest_data import format_cpr_for_sbsys
from utils.token_provider import OAuth2TokenProvider

logger = logging.getLogger(__name__)

_TIMEOUT = 30

class _PasswordGrantTokenProvider(OAuth2TokenProvider):
    def __init__(self, token_url, client_id, client_secret, username, password, timeout=20):
        super().__init__(token_url, client_id, client_secret, timeout=timeout)
        self.username = username
        self.password = password

    def acquire(self):
        with self._lock:
            return self._request_token({
                "grant_type": "password",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "username": self.username,
                "password": self.password
            })


class SbsysClient:
    def __init__(self, conn):
        extra = conn.extra_dejson
        required_extra = ("token_url", "client_id", "client_secret")
        if not conn.host or not conn.login or not conn.password or any(
            not extra.get(k) for k in required_extra):
                raise ValueError(
                     "SBSYS connection must include host, login, password and extra: token_url, client_id, client_secret"
                )
        self._base_url = conn.host.rstrip("/")
        self._tokens = _PasswordGrantTokenProvider(
             token_url=extra["token_url"],
             client_id=extra["client_id"],
             client_secret=extra["client_secret"],
             username=conn.login,
             password=conn.password,
        )
        self._session = requests.Session()

    def _request(self, method: str, path: str, **kwargs):
        url = f"{self._base_url}/{path.lstrip('/')}"
        response = self._session.request(
            method, url, headers={"Authorization": f"Bearer {self._tokens.get_token()}"}, timeout=_TIMEOUT, **kwargs
        )
        if response.status_code == 401:
            response = self._session.request(
                method, url, headers={"Authorization": f"Bearer {self._tokens.refresh()}"}, timeout=_TIMEOUT, **kwargs
            )
        # Response bodies are not logged: they may contain CPR/personal data
        response.raise_for_status()
        return response.json() if response.content else None

    def get_personalesag(self, cpr: str) -> list[dict]:
        payload = {
            "PrimaerPerson": {"CprNummer": format_cpr_for_sbsys(cpr)},
            "SagsTyper": [{"Navn": "Personalesag"}],
        }
        response = self._request("POST", "api/sag/search", json=payload)
        return (response or {}).get("Results") or []

    def get_delforloeb(self, sag_id: int) -> list[dict]:
        return self._request("GET", f"api/delforloeb/sag/{sag_id}") or []

    def journalize(
            self,
            pdf_bytes: bytes,
            sag_id: int,
            document_name: str,
            description: str,
            delforloeb_id: int | None = None,
    ) -> dict:
        metadata = { # JournaliserDokumentInputDtoV10
            "SagID": sag_id,
            "Beskribelse": description,
            "OmfattetAfAktindsigt": True,  # TODO: afklar for straffeattester
            "DokumentNavn": document_name,
        }
        files = {
            "file": (f"{document_name}.pdf", pdf_bytes, "application/pdf"),
            "json": (None, json.dumps(metadata, ensure_ascii=False), "application/json"),
        }
        path = "api/dokument/journaliser" + (f"/{delforloeb_id}" if delforloeb_id else "")
        return self._request("POST", path, files=files)