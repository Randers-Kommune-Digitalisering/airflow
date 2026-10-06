import json
import time
from typing import Any

import requests
from airflow.hooks.base import BaseHook


class SbsysClient:
    _instance: "SbsysClient | None" = None

    def __new__(cls, hook: BaseHook):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, hook: BaseHook):
        if getattr(self, "_initialized", False):
            return

        self._base_url = hook.host.rstrip("/")
        self._token_url = hook.extra_dejson["token_url"]
        if not self._base_url.startswith("https://") or not self._token_url.startswith("https://"):
            raise ValueError("SBSYS API and token URLs must use HTTPS")
        self._client_id = hook.login
        self._client_secret = hook.password
        self._username = hook.extra_dejson["username"]
        self._password = hook.extra_dejson["password"]
        self._session = requests.Session()
        self._access_token = None
        self._access_token_expiry = 0.0
        self._initialized = True

    def _request(self, method: str, path: str, **kwargs) -> Any:
        if time.monotonic() >= self._access_token_expiry:
            response = self._session.post(
                self._token_url,
                data={
                    "grant_type": "password",
                    "client_id": self._client_id,
                    "client_secret": self._client_secret,
                    "username": self._username,
                    "password": self._password,
                },
                timeout=20,
            )
            response.raise_for_status()
            token = response.json()
            self._access_token = token["access_token"]
            self._access_token_expiry = time.monotonic() + max(
                0, token["expires_in"] - 30
            )

        response = self._session.request(
            method,
            f"{self._base_url}/{path}",
            headers={"Authorization": f"Bearer {self._access_token}"},
            timeout=20,
            **kwargs,
        )
        response.raise_for_status()
        return response.json()

    def get_personalesag(self, cpr: str) -> list[dict]:
        if "-" not in cpr:
            cpr = f"{cpr[:6]}-{cpr[6:]}"

        payload = {
            "PrimaerPerson": {"CprNummer": cpr},
            "SagsTyper": [{"Navn": "PersonaleSag"}],
        }
        return self._request("POST", "api/sag/search", json=payload)["Results"]

    def get_emnesag(self, sag_id: int) -> dict:
        return self._request("GET", f"api/sag/{sag_id}")

    def get_delforloeb(self, sag_id: int) -> list[dict]:
        return self._request("GET", f"api/delforloeb/sag/{sag_id}")

    def get_delforloeb_from_sagid(self, sag_id: int) -> list[dict]:
        return self.get_delforloeb(sag_id)

    def journalize(self, file: bytes, sag_id: int, delforloeb_id: int | None = None) -> dict:
        metadata = {
            "SagID": sag_id,
            "Beskrivelse": "Fraværsbrev automatisk journaliseret.",
            "OmfattetAfAktindsigt": True,
            "DokumentNavn": "Fraværsbrev",
            "DokumentArt": {
                "Id": 1,
                "Navn": "Indgående",
            },
        }
        multipart = {
            "file": ("maindoc.pdf", file, "application/pdf"),
            "json": (None, json.dumps(metadata, ensure_ascii=False), "application/json"),
        }
        path = "api/dokument/journaliser"
        if delforloeb_id is not None:
            path = f"{path}/{delforloeb_id}"

        return self._request("POST", path, files=multipart)
