from __future__ import annotations

from dataclasses import dataclass
from email.message import EmailMessage
from typing import Any, Callable

import pytest


try:  # Prefer Airflow-style imports when dags/ is on sys.path
    from dag_kantinedata import process_kantinedata as pk_mod
    from dag_kantinedata.mail_utils import extract_attachments
except ImportError:  # Fallback when repo root is on sys.path
    from dags.dag_kantinedata import process_kantinedata as pk_mod
    from dags.dag_kantinedata.mail_utils import extract_attachments


@dataclass
class PutCall:
    remote_path: str
    content_bytes: bytes


class FakeSFTPClient:
    def __init__(
        self,
        *,
        put_impl: Callable[[bytes, str], None] | None = None,
        existing_paths: set[str] | None = None,
    ):
        self.put_calls: list[PutCall] = []
        self._put_impl = put_impl
        self._existing_paths: set[str] = set(existing_paths or set())

    def stat(self, remote_path: str) -> None:
        if remote_path not in self._existing_paths:
            raise FileNotFoundError(remote_path)

    def putfo(self, file_obj, remote_path: str) -> None:  # type: ignore[no-untyped-def]
        content = file_obj.read()
        if self._put_impl is not None:
            self._put_impl(content, remote_path)
        self._existing_paths.add(remote_path)
        self.put_calls.append(PutCall(remote_path=remote_path, content_bytes=content))

    def __enter__(self) -> "FakeSFTPClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        return False


class FakeSFTPHook:
    def __init__(self, conn_id: str, *, client: FakeSFTPClient):
        self.conn_id = conn_id
        self._client = client

    def get_conn(self) -> FakeSFTPClient:
        return self._client


class FakeVariable:
    def __init__(self, initial: dict[str, str] | None = None):
        self._store: dict[str, str] = dict(initial or {})

    def get(self, key: str, default_var: str | None = None) -> str | None:
        return self._store.get(key, default_var)

    def set(self, key: str, value: str) -> None:
        self._store[key] = value


class FakeEmailReader:
    def __init__(self, *, get_emails_impl: Callable[..., tuple[list[EmailMessage], list[bytes]]]):
        self._get_emails_impl = get_emails_impl

    def get_emails(self, **kwargs):  # type: ignore[no-untyped-def]
        return self._get_emails_impl(**kwargs)


def make_email_with_attachments(
    *,
    message_id: str = "<msg-1>",
    subject: str = "Test subject",
    uid: str = "1",
    attachments: list[tuple[bytes, str, str, str]] | None = None,
) -> EmailMessage:
    """Create an EmailMessage with attachments.

    attachments: list of (content_bytes, maintype, subtype, filename)
    """
    msg = EmailMessage()
    msg["Message-ID"] = message_id
    msg["Subject"] = subject
    msg.set_content("Body")

    # process_kantinedata expects a `.uid` attribute on each message.
    msg.uid = uid  # type: ignore[attr-defined]

    for content_bytes, maintype, subtype, filename in attachments or []:
        msg.add_attachment(
            content_bytes,
            maintype=maintype,
            subtype=subtype,
            filename=filename,
        )

    return msg


def test_extract_attachments_returns_xml_bytes_and_metadata() -> None:
    xml_bytes = b"<root><a>1</a></root>"
    msg = make_email_with_attachments(
        attachments=[(xml_bytes, "application", "xml", "data.xml")]
    )

    attachments = extract_attachments(msg)

    assert len(attachments) == 1
    assert attachments[0]["filename"] == "data.xml"
    assert attachments[0]["content_bytes"] == xml_bytes
    assert attachments[0]["content_type"] == "application/xml"
    assert attachments[0]["content_disposition"] == "attachment"


def test_unflags_mail_after_successful_processing_and_uploads_xml_bytes(monkeypatch) -> None:
    xml_bytes = b"<root><a>123</a></root>"
    msg = make_email_with_attachments(
        message_id="<msg-xml>",
        uid="123",
        attachments=[
            (xml_bytes, "application", "xml", "kantine.xml"),
            (b"ignored", "text", "plain", "note.txt"),
        ],
    )

    calls: list[dict[str, Any]] = []

    def fake_imap_get_emails_with_uids(**kwargs):  # type: ignore[no-untyped-def]
        calls.append(kwargs)
        criteria = kwargs.get("criteria")
        if criteria == "FLAGGED":
            return [], []
        if criteria == "UNSEEN":
            return [msg], []
        if isinstance(criteria, str) and criteria.startswith("UID "):
            return [], []
        raise AssertionError(f"Unexpected IMAP criteria: {criteria!r}")

    sftp_client = FakeSFTPClient()
    fake_var = FakeVariable({"kantinedata_file_counter": "0"})

    def fake_sftp_hook_factory(
        *args: Any,
        ssh_conn_id: str | None = None,
        **kwargs: Any,
    ) -> FakeSFTPHook:
        # Compatible with SFTPHook(conn_id) and SFTPHook(ssh_conn_id=...).
        # Keep accepting unused kwargs so signature stays resilient to upstream changes.
        _ = kwargs
        conn_id = ssh_conn_id
        if conn_id is None and args:
            conn_id = args[0]
        return FakeSFTPHook(conn_id or "", client=sftp_client)

    monkeypatch.setattr(
        pk_mod,
        "_get_email_reader",
        lambda: FakeEmailReader(get_emails_impl=fake_imap_get_emails_with_uids),
    )
    monkeypatch.setattr(pk_mod, "SFTPHook", fake_sftp_hook_factory)
    monkeypatch.setattr(pk_mod, "Variable", fake_var)

    pk_mod.process_kantinedata()

    # UNSEEN fetch should flag while processing
    assert any(
        c.get("criteria") == "UNSEEN" and c.get("set_flags") == "\\Flagged" for c in calls
    )

    # XML attachment uploaded byte-for-byte, with correct remote path
    assert len(sftp_client.put_calls) == 1
    assert sftp_client.put_calls[0].remote_path == "/EksportedeOrdrer_1.xml"
    assert sftp_client.put_calls[0].content_bytes == xml_bytes

    # Success should trigger unflag of the processed UID
    assert any(
        c.get("del_flags") == "\\Flagged" and c.get("criteria") == "UID 123" for c in calls
    )


def test_mails_remain_flagged_if_sftp_upload_errors(monkeypatch) -> None:
    xml_bytes = b"<root />"
    msg = make_email_with_attachments(
        message_id="<msg-fail>",
        uid="7",
        attachments=[(xml_bytes, "application", "xml", "fail.xml")],
    )

    calls: list[dict[str, Any]] = []

    def fake_imap_get_emails_with_uids(**kwargs):  # type: ignore[no-untyped-def]
        calls.append(kwargs)
        criteria = kwargs.get("criteria")
        if criteria == "FLAGGED":
            return [], []
        if criteria == "UNSEEN":
            return [msg], []
        if isinstance(criteria, str) and criteria.startswith("UID "):
            pytest.fail("Unflagging should not happen when processing fails")
        raise AssertionError(f"Unexpected IMAP criteria: {criteria!r}")

    def raising_put_impl(_content: bytes, _remote_path: str) -> None:
        raise OSError("SFTP down")

    sftp_client = FakeSFTPClient(put_impl=raising_put_impl)
    fake_var = FakeVariable({"kantinedata_file_counter": "0"})

    def fake_sftp_hook_factory(
        *args: Any,
        ssh_conn_id: str | None = None,
        **kwargs: Any,
    ) -> FakeSFTPHook:
        _ = kwargs
        conn_id = ssh_conn_id
        if conn_id is None and args:
            conn_id = args[0]
        return FakeSFTPHook(conn_id or "", client=sftp_client)

    monkeypatch.setattr(
        pk_mod,
        "_get_email_reader",
        lambda: FakeEmailReader(get_emails_impl=fake_imap_get_emails_with_uids),
    )
    monkeypatch.setattr(pk_mod, "SFTPHook", fake_sftp_hook_factory)
    monkeypatch.setattr(pk_mod, "Variable", fake_var)

    with pytest.raises(Exception, match=r"Failed to process 1 email\(s\)\. UIDs: 7"):
        pk_mod.process_kantinedata()

    # Mail must have been flagged for processing
    assert any(
        c.get("criteria") == "UNSEEN" and c.get("set_flags") == "\\Flagged" for c in calls
    )


def test_flagged_mail_is_retrieved_on_rerun_after_previous_failure(monkeypatch) -> None:
    xml_bytes = b"<root>rerun</root>"
    msg = make_email_with_attachments(
        message_id="<msg-rerun>",
        uid="42",
        attachments=[(xml_bytes, "application", "xml", "rerun.xml")],
    )

    calls: list[dict[str, Any]] = []
    run_state = {"run": 1}

    def fake_imap_get_emails_with_uids(**kwargs):  # type: ignore[no-untyped-def]
        calls.append(kwargs)
        criteria = kwargs.get("criteria")

        if run_state["run"] == 1:
            if criteria == "FLAGGED":
                return [], []
            if criteria == "UNSEEN":
                return [msg], []
            if isinstance(criteria, str) and criteria.startswith("UID "):
                pytest.fail("First run fails, so unflagging should not happen")
        else:
            if criteria == "FLAGGED":
                return [msg], []
            if criteria == "UNSEEN":
                return [], []
            if isinstance(criteria, str) and criteria.startswith("UID "):
                return [], []

        raise AssertionError(f"Unexpected IMAP criteria in run {run_state['run']}: {criteria!r}")

    put_call_count = {"n": 0}

    def put_impl(_content: bytes, _remote_path: str) -> None:
        put_call_count["n"] += 1
        if put_call_count["n"] == 1:
            raise OSError("Transient SFTP error")

    sftp_client = FakeSFTPClient(put_impl=put_impl)
    fake_var = FakeVariable({"kantinedata_file_counter": "0"})

    def fake_sftp_hook_factory(
        *args: Any,
        ssh_conn_id: str | None = None,
        **kwargs: Any,
    ) -> FakeSFTPHook:
        _ = kwargs
        conn_id = ssh_conn_id
        if conn_id is None and args:
            conn_id = args[0]
        return FakeSFTPHook(conn_id or "", client=sftp_client)

    monkeypatch.setattr(
        pk_mod,
        "_get_email_reader",
        lambda: FakeEmailReader(get_emails_impl=fake_imap_get_emails_with_uids),
    )
    monkeypatch.setattr(pk_mod, "SFTPHook", fake_sftp_hook_factory)
    monkeypatch.setattr(pk_mod, "Variable", fake_var)

    with pytest.raises(Exception):
        pk_mod.process_kantinedata()

    run_state["run"] = 2
    pk_mod.process_kantinedata()

    # Second run should fetch the mail via FLAGGED (rerun behavior)
    assert any(
        c.get("criteria") == "FLAGGED" for c in calls[2:4]
    ), "Expected second run to query FLAGGED mailbox"

    # Second run should unflag UID 42 after success
    assert any(
        c.get("del_flags") == "\\Flagged" and c.get("criteria") == "UID 42" for c in calls
    )


def test_unflagging_dedupes_and_sorts_uids(monkeypatch) -> None:
    msg1 = make_email_with_attachments(message_id="<m1>", uid="1", attachments=[])
    msg2 = make_email_with_attachments(message_id="<m2>", uid="2", attachments=[])

    calls: list[dict[str, Any]] = []

    def fake_imap_get_emails_with_uids(**kwargs):  # type: ignore[no-untyped-def]
        calls.append(kwargs)
        criteria = kwargs.get("criteria")
        if criteria == "FLAGGED":
            # include UID 2
            return [msg2], []
        if criteria == "UNSEEN":
            # include UID 1 and a duplicate UID 2
            return [msg1, msg2], []
        if isinstance(criteria, str) and criteria.startswith("UID "):
            return [], []
        raise AssertionError(f"Unexpected IMAP criteria: {criteria!r}")

    # SFTP should never be used because there are no attachments.
    def fail_sftp_hook_factory(ssh_conn_id: str | None = None, *args: Any, **kwargs: Any):
        pytest.fail("SFTPHook should not be created when there are no XML attachments")

    monkeypatch.setattr(
        pk_mod,
        "_get_email_reader",
        lambda: FakeEmailReader(get_emails_impl=fake_imap_get_emails_with_uids),
    )
    monkeypatch.setattr(pk_mod, "SFTPHook", fail_sftp_hook_factory)

    pk_mod.process_kantinedata()

    # Expect a single unflag call with sorted, de-duplicated UIDs.
    assert any(
        c.get("del_flags") == "\\Flagged" and c.get("criteria") == "UID 1,2" for c in calls
    )


def test_no_new_emails_short_circuits(monkeypatch) -> None:
    calls: list[dict[str, Any]] = []

    def fake_imap_get_emails_with_uids(**kwargs):  # type: ignore[no-untyped-def]
        calls.append(kwargs)
        criteria = kwargs.get("criteria")
        if criteria in {"FLAGGED", "UNSEEN"}:
            return [], []
        pytest.fail(f"No further IMAP calls expected, got: {criteria!r}")

    def fail_sftp_hook_factory(ssh_conn_id: str | None = None, *args: Any, **kwargs: Any):
        pytest.fail("SFTPHook should not be created when there are no emails")

    monkeypatch.setattr(
        pk_mod,
        "_get_email_reader",
        lambda: FakeEmailReader(get_emails_impl=fake_imap_get_emails_with_uids),
    )
    monkeypatch.setattr(pk_mod, "SFTPHook", fail_sftp_hook_factory)

    pk_mod.process_kantinedata()

    assert [c.get("criteria") for c in calls] == ["FLAGGED", "UNSEEN"]


def test_imap_fetch_exception_is_raised(monkeypatch, caplog) -> None:
    def raising_imap_get_emails_with_uids(**_kwargs):  # type: ignore[no-untyped-def]
        raise RuntimeError("IMAP failure")

    monkeypatch.setattr(
        pk_mod,
        "_get_email_reader",
        lambda: FakeEmailReader(get_emails_impl=raising_imap_get_emails_with_uids),
    )

    with pytest.raises(RuntimeError, match="IMAP failure"):
        pk_mod.process_kantinedata()

    assert any("Error fetching emails" in rec.message for rec in caplog.records)


def test_sftp_hook_init_failure_is_raised_and_mail_not_unflagged(monkeypatch) -> None:
    xml_bytes = b"<root />"
    msg = make_email_with_attachments(
        message_id="<msg-sftp-init-fail>",
        uid="99",
        attachments=[(xml_bytes, "application", "xml", "initfail.xml")],
    )

    calls: list[dict[str, Any]] = []

    def fake_imap_get_emails_with_uids(**kwargs):  # type: ignore[no-untyped-def]
        calls.append(kwargs)
        criteria = kwargs.get("criteria")
        if criteria == "FLAGGED":
            return [], []
        if criteria == "UNSEEN":
            return [msg], []
        if isinstance(criteria, str) and criteria.startswith("UID "):
            pytest.fail("Unflagging should not happen when SFTP hook init fails")
        raise AssertionError(f"Unexpected IMAP criteria: {criteria!r}")

    def raising_sftp_hook_factory(ssh_conn_id: str | None = None, *args: Any, **kwargs: Any):
        raise RuntimeError("SFTP credentials invalid")

    fake_var = FakeVariable({"kantinedata_file_counter": "0"})

    monkeypatch.setattr(
        pk_mod,
        "_get_email_reader",
        lambda: FakeEmailReader(get_emails_impl=fake_imap_get_emails_with_uids),
    )
    monkeypatch.setattr(pk_mod, "SFTPHook", raising_sftp_hook_factory)
    monkeypatch.setattr(pk_mod, "Variable", fake_var)

    with pytest.raises(Exception, match=r"Failed to process 1 email\(s\)\. UIDs: 99"):
        pk_mod.process_kantinedata()

    # UNSEEN fetch should flag while processing (mail remains flagged for rerun)
    assert any(
        c.get("criteria") == "UNSEEN" and c.get("set_flags") == "\\Flagged" for c in calls
    )


def test_filename_counter_skips_existing_remote_path(monkeypatch) -> None:
    xml_bytes = b"<root><a>1</a></root>"
    msg = make_email_with_attachments(
        message_id="<msg-skip-existing>",
        uid="1",
        attachments=[(xml_bytes, "application", "xml", "ignored.xml")],
    )

    def fake_imap_get_emails_with_uids(**kwargs):  # type: ignore[no-untyped-def]
        criteria = kwargs.get("criteria")
        if criteria == "FLAGGED":
            return [], []
        if criteria == "UNSEEN":
            return [msg], []
        if isinstance(criteria, str) and criteria.startswith("UID "):
            return [], []
        raise AssertionError(f"Unexpected IMAP criteria: {criteria!r}")

    # Simulate that EksportedeOrdrer_1.xml already exists on SFTP
    sftp_client = FakeSFTPClient(existing_paths={"/EksportedeOrdrer_1.xml"})

    def fake_sftp_hook_factory(
        *args: Any,
        ssh_conn_id: str | None = None,
        **kwargs: Any,
    ) -> FakeSFTPHook:
        _ = kwargs
        conn_id = ssh_conn_id
        if conn_id is None and args:
            conn_id = args[0]
        return FakeSFTPHook(conn_id or "", client=sftp_client)

    fake_var = FakeVariable({"kantinedata_file_counter": "0"})

    monkeypatch.setattr(
        pk_mod,
        "_get_email_reader",
        lambda: FakeEmailReader(get_emails_impl=fake_imap_get_emails_with_uids),
    )
    monkeypatch.setattr(pk_mod, "SFTPHook", fake_sftp_hook_factory)
    monkeypatch.setattr(pk_mod, "Variable", fake_var)

    pk_mod.process_kantinedata()

    assert len(sftp_client.put_calls) == 1
    assert sftp_client.put_calls[0].remote_path == "/EksportedeOrdrer_2.xml"


def test_filename_counter_wraps_after_10(monkeypatch) -> None:
    sftp_client = FakeSFTPClient()
    fake_var = FakeVariable({"kantinedata_file_counter": "10"})
    monkeypatch.setattr(pk_mod, "Variable", fake_var)

    filename = pk_mod._allocate_next_filename(sftp_client)

    assert filename == "EksportedeOrdrer_1.xml"


def test_filename_counter_wraps_and_skips_existing(monkeypatch) -> None:
    sftp_client = FakeSFTPClient(existing_paths={"/EksportedeOrdrer_1.xml"})
    fake_var = FakeVariable({"kantinedata_file_counter": "10"})
    monkeypatch.setattr(pk_mod, "Variable", fake_var)

    filename = pk_mod._allocate_next_filename(sftp_client)

    assert filename == "EksportedeOrdrer_2.xml"


def test_filename_counter_raises_when_all_10_slots_taken(monkeypatch) -> None:
    existing = {f"/EksportedeOrdrer_{i}.xml" for i in range(1, 11)}
    sftp_client = FakeSFTPClient(existing_paths=existing)
    fake_var = FakeVariable({"kantinedata_file_counter": "10"})
    monkeypatch.setattr(pk_mod, "Variable", fake_var)

    with pytest.raises(RuntimeError, match=r"No available Kantinedata filename slots"):
        pk_mod._allocate_next_filename(sftp_client)
