import logging
import random
import time

import requests
from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)

MUNICIPALITY_CODE = 730


class AdressevaelgerClient:
    def __init__(self):
        connection = BaseHook.get_connection("adressevaelger_api")
        self.base_url = connection.host.rstrip("/")
        self.token = connection.password
        self.session = requests.Session()

    def _fetch(self, adresse_id: str) -> dict | None:
        """
        Fetch the raw Adressevælger payload for an address, with retries on transient errors.

        :param adresse_id: The DAR address UUID (id_lokalid).
        :return: The parsed JSON payload, or None if the address was not found or all attempts failed.
        """
        url = f"{self.base_url}/adresser/{adresse_id}"
        params = {"token": self.token}
        max_retries = 4
        base_backoff_seconds = 0.5

        for attempt in range(max_retries + 1):
            try:
                response = self.session.get(url, params=params, timeout=10)
                if response.status_code == 404:
                    logger.error(
                        "Adressevælger lookup for adresse_id=%s returned 404. Skipping address/district lookup for this user.",
                        adresse_id,
                    )
                    return None
                response.raise_for_status()
                return response.json()
            except Exception as e:
                # Do not log str(e): requests errors include the full URL with the token.
                error_desc = type(e).__name__
                status_code = getattr(getattr(e, "response", None), "status_code", None)
                if status_code is not None:
                    error_desc += f" (HTTP {status_code})"

                if attempt == max_retries:
                    logger.error(
                        "Adressevælger lookup for adresse_id=%s failed after %d attempts: %s. Skipping address/district lookup for this user.",
                        adresse_id,
                        max_retries + 1,
                        error_desc,
                    )
                    return None

                sleep_seconds = base_backoff_seconds * (2 ** attempt) + random.uniform(0, 0.25)
                logger.warning(
                    "Adressevælger lookup for adresse_id=%s failed on attempt %d/%d: %s. Retrying in %.2fs.",
                    adresse_id,
                    attempt + 1,
                    max_retries + 1,
                    error_desc,
                    sleep_seconds,
                )
                time.sleep(sleep_seconds)
        return None

    def get_address_by_id(self, adresse_id: str) -> dict | None:
        """
        Get address information from Adressevælger by adresse_id.

        :param adresse_id: The DAR address UUID (id_lokalid).
        :return: A dictionary containing:
            full_address(str),
            number_floor(str),
            street_code(int),
            town_name(str),
            postal_code(int),
            municipality_code(int),
            coordinates(tuple[float, float])
            OR None if the address is not found, not in the municipality, or the lookup fails.
        """
        data = self._fetch(adresse_id)
        if data is None:
            return None

        status = data.get("status") if isinstance(data, dict) else None
        address_data = data.get("adresse") if isinstance(data, dict) else None
        if status != "ok" or not address_data:
            logger.error(
                "Adressevælger lookup for adresse_id=%s returned status=%r without address data. Skipping address/district lookup for this user.",
                adresse_id,
                status,
            )
            return None

        house = address_data.get("husnummer") or {}
        street_municipality = house.get("navngivenvejkommunedel") or {}
        postal = house.get("postnummer") or {}
        town = house.get("supplerendebynavn") or {}
        access_point = house.get("adgangspunkt") or {}

        municipality_code_raw = street_municipality.get("kommune")
        if municipality_code_raw is None or int(municipality_code_raw) != MUNICIPALITY_CODE:
            logger.error(
                "Adressevælger lookup for adresse_id=%s returned an address outside municipality %s. Skipping address/district lookup for this user.",
                adresse_id,
                MUNICIPALITY_CODE,
            )
            return None

        full_address = address_data.get("adressebetegnelse") or ""

        number = house.get("husnummertekst")
        floor = address_data.get("etagebetegnelse")
        door = address_data.get("doerbetegnelse")

        number_floor = f"{number}" if number else ""
        if floor:
            number_floor += f", {floor}"
        if door:
            number_floor += f" {door}"

        street_code_raw = street_municipality.get("vejkode")
        postal_code_raw = postal.get("postnr")
        coords = access_point.get("koordinater") or {}
        x, y = coords.get("x"), coords.get("y")
        if x is None or y is None:
            geometry_coords = (access_point.get("geometri") or {}).get("coordinates") or []
            if len(geometry_coords) == 2:
                x, y = geometry_coords

        if not all([full_address, number_floor, street_code_raw, postal_code_raw, x, y]):
            raise ValueError(f"A required address field is missing in response for adresse_id {adresse_id}")

        return {
            "full_address": full_address,
            "number_floor": number_floor,
            "street_code": int(street_code_raw),
            "town_name": town.get("navn") or "",  # optional
            "postal_code": int(postal_code_raw),
            "municipality_code": int(municipality_code_raw),
            "coordinates": (float(x), float(y)),
        }
