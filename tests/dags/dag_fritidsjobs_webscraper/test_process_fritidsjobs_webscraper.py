import pytest

import dag_fritidsjobs_webscraper.process_fritidsjobs_webscraper as process_module


class FakeSession:
    def close(self) -> None:
        return None


class FakeDatabaseManager:
    def __init__(self, *args, **kwargs) -> None:
        _ = args, kwargs

    def get_session(self) -> FakeSession:
        return FakeSession()


def _runtime_config(recipients):
    return {
        "sites": [],
        "sender_email": "noreply@randers.dk",
        "recipient_emails": recipients,
        "smtp_server": "smtp.example.com",
    }


def _patch_dependencies(monkeypatch, recipients) -> None:
    monkeypatch.setattr(process_module.Variable, "get", lambda *args, **kwargs: _runtime_config(recipients))
    monkeypatch.setattr(process_module, "DatabaseManager", FakeDatabaseManager)
    monkeypatch.setattr(process_module, "scrape_sites", lambda _site_configs: [])
    monkeypatch.setattr(process_module, "filter_existing_jobs", lambda _db_session, _scraped_sites: [])
    monkeypatch.setattr(process_module, "construct_email", lambda _jobs: ("Subject", "Body"))


@pytest.mark.parametrize(
    "recipients",
    [
        "person@example.com",
        ("Person", "person@example.com"),
        ["person1@example.com", "person2@example.com"],
        ["person1@example.com", ("Person", "person2@example.com")],
    ],
)
def test_process_fritidsjobs_webscraper_accepts_all_supported_recipient_formats(monkeypatch, recipients) -> None:
    _patch_dependencies(monkeypatch, recipients)

    result = process_module.process_fritidsjobs_webscraper()

    assert result is None


def test_process_fritidsjobs_webscraper_raises_for_invalid_recipient_format(monkeypatch) -> None:
    _patch_dependencies(monkeypatch, [123])

    with pytest.raises(TypeError, match="recipient"):
        process_module.process_fritidsjobs_webscraper()
