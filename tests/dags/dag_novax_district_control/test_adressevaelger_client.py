import copy
from types import SimpleNamespace

import pytest

from airflow.exceptions import AirflowFailException

from dag_novax_district_control.clients import adressevaelger_client as module
from dag_novax_district_control.clients.adressevaelger_client import AdressevaelgerClient

ADDRESS_ID = "0a3f50c0-6cda-32b8-e044-0003ba298018"

SAMPLE_RESPONSE = {
    "status": "ok",
    "adresse": {
        "id_lokalid": ADDRESS_ID,
        "adressebetegnelse": "Holkvej 11, Mellerup, 8930 Randers NØ",
        "etagebetegnelse": None,
        "doerbetegnelse": None,
        "husnummer": {
            "husnummertekst": "11",
            "vejnavn": "Holkvej",
            "adgangspunkt": {
                "geometri": {"type": "Point", "coordinates": [574370, 6265111]},
                "koordinater": {"x": 574370, "y": 6265111},
            },
            "postnummer": {"navn": "Randers NØ", "postnr": "8930"},
            "navngivenvejkommunedel": {"kommune": "0730", "vejkode": "0850"},
            "supplerendebynavn": {"navn": "Mellerup"},
        },
    },
}


class _FakeResponse:
    def __init__(self, payload, status_code: int = 200):
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            import requests

            raise requests.HTTPError(f"{self.status_code} for url: ...?token=secret", response=self)

    def json(self):
        return self._payload


@pytest.fixture
def client(monkeypatch) -> AdressevaelgerClient:
    monkeypatch.setattr(module.time, "sleep", lambda _s: None)
    connection = SimpleNamespace(host="https://adressevaelger.example/", password="test-token")
    monkeypatch.setattr(module.BaseHook, "get_connection", lambda conn_id: connection)
    return AdressevaelgerClient()


def _patch_get(monkeypatch, client, responses):
    calls = []
    iterator = iter(responses)

    def fake_get(url, params=None, timeout=None):
        calls.append((url, params))
        return next(iterator)

    monkeypatch.setattr(client.session, "get", fake_get)
    return calls


def test_get_address_by_id_maps_response(monkeypatch, client) -> None:
    calls = _patch_get(monkeypatch, client, [_FakeResponse(SAMPLE_RESPONSE)])

    result = client.get_address_by_id(ADDRESS_ID)

    assert calls == [(f"https://adressevaelger.example/adresser/{ADDRESS_ID}", {"token": "test-token"})]
    assert result == {
        "full_address": "Holkvej 11, Mellerup, 8930 Randers NØ",
        "number_floor": "11",
        "street_code": 850,
        "town_name": "Mellerup",
        "postal_code": 8930,
        "municipality_code": 730,
        "coordinates": (574370.0, 6265111.0),
    }


def test_get_address_by_id_includes_floor_and_door(monkeypatch, client) -> None:
    payload = copy.deepcopy(SAMPLE_RESPONSE)
    payload["adresse"]["etagebetegnelse"] = "st"
    payload["adresse"]["doerbetegnelse"] = "tv"
    payload["adresse"]["husnummer"]["supplerendebynavn"] = None
    _patch_get(monkeypatch, client, [_FakeResponse(payload)])

    result = client.get_address_by_id(ADDRESS_ID)

    assert result["number_floor"] == "11, st tv"
    assert result["town_name"] == ""


def test_get_address_by_id_falls_back_to_geometry_coordinates(monkeypatch, client) -> None:
    payload = copy.deepcopy(SAMPLE_RESPONSE)
    payload["adresse"]["husnummer"]["adgangspunkt"]["koordinater"] = None
    _patch_get(monkeypatch, client, [_FakeResponse(payload)])

    assert client.get_address_by_id(ADDRESS_ID)["coordinates"] == (574370.0, 6265111.0)


def test_get_address_by_id_returns_none_outside_municipality(monkeypatch, client) -> None:
    payload = copy.deepcopy(SAMPLE_RESPONSE)
    payload["adresse"]["husnummer"]["navngivenvejkommunedel"]["kommune"] = "0751"
    _patch_get(monkeypatch, client, [_FakeResponse(payload)])

    assert client.get_address_by_id(ADDRESS_ID) is None


def test_get_address_by_id_returns_none_on_non_ok_status(monkeypatch, client) -> None:
    _patch_get(monkeypatch, client, [_FakeResponse({"status": "error"})])

    assert client.get_address_by_id(ADDRESS_ID) is None


def test_get_address_by_id_returns_none_on_404_without_retry(monkeypatch, client) -> None:
    calls = _patch_get(monkeypatch, client, [_FakeResponse({}, status_code=404)])

    assert client.get_address_by_id(ADDRESS_ID) is None
    assert len(calls) == 1


def test_get_address_by_id_retries_then_succeeds(monkeypatch, client) -> None:
    calls = _patch_get(
        monkeypatch,
        client,
        [_FakeResponse({}, status_code=503), _FakeResponse(SAMPLE_RESPONSE)],
    )

    assert client.get_address_by_id(ADDRESS_ID)["street_code"] == 850
    assert len(calls) == 2


@pytest.mark.parametrize("status_code", [400, 401, 403])
def test_get_address_by_id_raises_on_non_transient_4xx_without_retry(monkeypatch, client, status_code) -> None:
    calls = _patch_get(monkeypatch, client, [_FakeResponse({}, status_code=status_code)])

    with pytest.raises(AirflowFailException) as exc_info:
        client.get_address_by_id(ADDRESS_ID)

    assert len(calls) == 1
    assert str(status_code) in str(exc_info.value)
    assert "token" not in str(exc_info.value)
    assert exc_info.value.__cause__ is None


def test_get_address_by_id_retries_on_429(monkeypatch, client) -> None:
    calls = _patch_get(
        monkeypatch,
        client,
        [_FakeResponse({}, status_code=429), _FakeResponse(SAMPLE_RESPONSE)],
    )

    assert client.get_address_by_id(ADDRESS_ID)["street_code"] == 850
    assert len(calls) == 2


def test_get_address_by_id_does_not_log_token(monkeypatch, client, caplog) -> None:
    _patch_get(monkeypatch, client, [_FakeResponse({}, status_code=500)] * 5)

    with caplog.at_level("WARNING"):
        assert client.get_address_by_id(ADDRESS_ID) is None

    assert "token" not in caplog.text


def test_get_address_by_id_raises_on_missing_required_field(monkeypatch, client) -> None:
    payload = copy.deepcopy(SAMPLE_RESPONSE)
    payload["adresse"]["husnummer"]["postnummer"] = None
    _patch_get(monkeypatch, client, [_FakeResponse(payload)])

    with pytest.raises(ValueError):
        client.get_address_by_id(ADDRESS_ID)
