import json
from types import SimpleNamespace

import pytest
import requests

from dag_xflow_value_lists import sync_value_lists as module


LIST_ONE = "f00f8e6e-13c4-48ef-a36c-6c238910a543"
LIST_TWO = "80362131-e79e-44f6-818a-acdd5fca9ce6"
LIST_THREE = "27aa9285-042b-4bdd-994e-9576701a2e5e"
SOURCE_URL = "https://raw.githubusercontent.com/org/repo/main/list.json"


class FakeResponse:
    def __init__(self, content=b"", status_code=200):
        self.content = content
        self.status_code = status_code
        self.closed = False

    def iter_content(self, chunk_size):
        for start in range(0, len(self.content), chunk_size):
            yield self.content[start:start + chunk_size]

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError("HTTP request failed")

    def close(self):
        self.closed = True


class FakeSession:
    instances = []

    def __init__(self):
        self.headers = {}
        self.calls = []
        self.instances.append(self)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def get(self, url, **kwargs):
        self.calls.append(("GET", url, kwargs))
        return FakeResponse(json.dumps(["remote one", "remote two"]).encode())

    def put(self, url, **kwargs):
        self.calls.append(("PUT", url, kwargs))
        return FakeResponse(status_code=204)


@pytest.fixture(autouse=True)
def mock_network(monkeypatch):
    FakeSession.instances = []
    monkeypatch.setattr(module.requests, "Session", FakeSession)
    monkeypatch.setattr(
        module.BaseHook,
        "get_connection",
        lambda conn_id: SimpleNamespace(host="https://api.test.xflow.dk/", password="secret"),
    )
    monkeypatch.setattr(
        module.Variable,
        "get",
        lambda key, default_var=None: json.dumps(["airflow one"]) if key == "source_values" else default_var,
    )


def config(entries, allowed_hosts=None, created_by="RPA"):
    return json.dumps({"allowed_url_hosts": allowed_hosts or [], "created_by": created_by, "sync": entries})


def inline(list_id=LIST_ONE, values=None):
    return {"list_id": list_id, "origin": "values", "values": values if values is not None else ["one", "two"]}


def test_sync_three_sources_prepares_all_then_replaces():
    entries = [
        inline(),
        {"list_id": LIST_TWO, "origin": "url", "url": SOURCE_URL},
        {"list_id": LIST_THREE, "origin": "airflow", "airflow": "source_values"},
    ]
    module.sync_value_lists(config(entries, ["raw.githubusercontent.com"]))

    source, destination = FakeSession.instances
    assert source.headers == {}
    assert source.calls == [("GET", SOURCE_URL, {"timeout": module.TIMEOUT, "allow_redirects": False, "stream": True})]
    assert destination.headers == {"publicApiToken": "secret"}
    assert [call[1] for call in destination.calls] == [
        f"https://api.test.xflow.dk/ValueList/{list_id}/Values" for list_id in (LIST_ONE, LIST_TWO, LIST_THREE)
    ]
    assert destination.calls[0][2] == {
        "json": {"ValueListData": [
            {"key": "1", "indeks": "1", "value": "one", "oprettetAf": "RPA"},
            {"key": "2", "indeks": "2", "value": "two", "oprettetAf": "RPA"},
        ]},
        "timeout": module.TIMEOUT,
        "allow_redirects": False,
    }


def test_created_by_is_read_from_runtime_config():
    module.sync_value_lists(config([inline()], created_by="value-list-job"))

    destination = FakeSession.instances[-1]
    items = destination.calls[0][2]["json"]["ValueListData"]
    assert [item["oprettetAf"] for item in items] == ["value-list-job", "value-list-job"]


@pytest.mark.parametrize("entries,hosts", [
    ([inline(values=[])], []),
    ([inline(values=[" "])], []),
    ([inline(values=[1])], []),
    ([{"list_id": LIST_ONE, "origin": [], "values": ["one"]}], []),
    ([inline(), inline()], []),
    ([{"list_id": LIST_ONE, "origin": "url", "url": SOURCE_URL}], []),
    ([{"list_id": LIST_ONE, "origin": "url", "url": "http://raw.githubusercontent.com/a"}], ["raw.githubusercontent.com"]),
    ([{"list_id": LIST_ONE, "origin": "url", "url": "https://raw.githubusercontent.com.evil.example/a"}], ["raw.githubusercontent.com"]),
    ([{"list_id": LIST_ONE, "origin": "url", "url": "https://user@raw.githubusercontent.com/a"}], ["raw.githubusercontent.com"]),
    ([{"list_id": LIST_ONE, "origin": "url", "url": "https://@raw.githubusercontent.com/a"}], ["raw.githubusercontent.com"]),
    ([{"list_id": LIST_ONE, "origin": "url", "url": "https://raw.githubusercontent.com/a?token=x"}], ["raw.githubusercontent.com"]),
    ([{"list_id": LIST_ONE, "origin": "url", "url": SOURCE_URL}], ["https://raw.githubusercontent.com"]),
])
def test_invalid_configuration_prevents_all_requests(entries, hosts):
    with pytest.raises(ValueError):
        module.sync_value_lists(config(entries, hosts))
    assert FakeSession.instances == []


def test_blank_created_by_prevents_all_requests():
    with pytest.raises(ValueError, match="created_by"):
        module.sync_value_lists(config([inline()], created_by=" "))
    assert FakeSession.instances == []


def test_missing_airflow_source_prevents_all_puts():
    entries = [inline(), {"list_id": LIST_TWO, "origin": "airflow", "airflow": "missing"}]
    with pytest.raises(ValueError, match="missing"):
        module.sync_value_lists(config(entries))
    assert len(FakeSession.instances) == 1


def test_redirected_url_prevents_all_puts(monkeypatch):
    monkeypatch.setattr(FakeSession, "get", lambda self, url, **kwargs: FakeResponse(status_code=302))
    entries = [inline(), {"list_id": LIST_TWO, "origin": "url", "url": SOURCE_URL}]
    with pytest.raises(ValueError, match="redirect"):
        module.sync_value_lists(config(entries, ["raw.githubusercontent.com"]))
    assert len(FakeSession.instances) == 1


def test_oversized_url_prevents_all_puts(monkeypatch):
    monkeypatch.setattr(FakeSession, "get", lambda self, url, **kwargs: FakeResponse(b"x" * (module.MAX_SOURCE_BYTES + 1)))
    entries = [{"list_id": LIST_ONE, "origin": "url", "url": SOURCE_URL}]
    with pytest.raises(ValueError, match="size limit"):
        module.sync_value_lists(config(entries, ["raw.githubusercontent.com"]))
    assert len(FakeSession.instances) == 1


def test_xflow_http_error_fails_sync(monkeypatch):
    monkeypatch.setattr(FakeSession, "put", lambda self, url, **kwargs: FakeResponse(status_code=500))
    with pytest.raises(requests.HTTPError):
        module.sync_value_lists(config([inline()]))
