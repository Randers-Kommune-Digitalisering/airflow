from email.message import EmailMessage

import pytest

import dag_aub_post.process_aub_post as process_module


class FakeConnection:
    host = "imap.example.com"
    login = "mailbox@randers.dk"
    password = "secret"
    port = 143
    extra_dejson = {}


class FakeEmailReader:
    emails = []
    failed_ids = []
    deleted = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def get_emails(self, mailbox, criteria, set_flags, del_flags):
        return list(self.emails), list(self.failed_ids)

    def delete_email_by_uid(self, uid, mailbox, expunge):
        self.deleted.append((uid, mailbox, expunge))


class FakeEmailSender:
    sent_messages = []
    should_fail = False

    def __init__(self, smtp_server):
        self.smtp_server = smtp_server

    def send_email(self, **kwargs):
        if self.should_fail:
            raise RuntimeError("SMTP send failed")
        self.sent_messages.append(kwargs)


def _make_email(uid: bytes, filename: str = "maindoc.pdf") -> EmailMessage:
    message = EmailMessage()
    message["Subject"] = "AUB"
    message.set_content("Mail body")
    message.add_attachment(
        b"pdf-bytes",
        maintype="application",
        subtype="pdf",
        filename=filename,
    )
    message.uid = uid
    return message


def _runtime_config() -> dict:
    return {
        "smtp_server": "smtp.example.com",
        "sender_email": "noreply@randers.dk",
        "contacts_map": [
            {
                "email": "kontakt@randers.dk",
                "educations": ["Pædagog"],
            }
        ],
        "mailbox": "INBOX",
        "mail_search_criteria": "ALL",
    }


def _set_defaults() -> None:
    FakeEmailReader.emails = []
    FakeEmailReader.failed_ids = []
    FakeEmailReader.deleted = []
    FakeEmailSender.sent_messages = []
    FakeEmailSender.should_fail = False


def test_process_aub_post_success_deletes_email(monkeypatch) -> None:
    _set_defaults()
    FakeEmailReader.emails = [_make_email(uid=b"1")]

    monkeypatch.setattr(process_module.Variable, "get", lambda *args, **kwargs: _runtime_config())
    monkeypatch.setattr(process_module.BaseHook, "get_connection", lambda _id: FakeConnection())
    monkeypatch.setattr(process_module, "EmailReader", FakeEmailReader)
    monkeypatch.setattr(process_module, "EmailSender", FakeEmailSender)
    monkeypatch.setattr(process_module, "extract_education_from_pdf", lambda _bytes: "Pædagog")

    process_module.process_aub_post()

    assert len(FakeEmailSender.sent_messages) == 1
    assert FakeEmailSender.sent_messages[0]["subject"] == "AUB"
    assert FakeEmailSender.sent_messages[0]["body"].strip() == "Mail body"
    assert FakeEmailReader.deleted == [(b"1", "INBOX", True)]


def test_process_aub_post_send_failure_raises_and_does_not_delete(monkeypatch) -> None:
    _set_defaults()
    FakeEmailReader.emails = [_make_email(uid=b"2")]
    FakeEmailSender.should_fail = True

    monkeypatch.setattr(process_module.Variable, "get", lambda *args, **kwargs: _runtime_config())
    monkeypatch.setattr(process_module.BaseHook, "get_connection", lambda _id: FakeConnection())
    monkeypatch.setattr(process_module, "EmailReader", FakeEmailReader)
    monkeypatch.setattr(process_module, "EmailSender", FakeEmailSender)
    monkeypatch.setattr(process_module, "extract_education_from_pdf", lambda _bytes: "Pædagog")

    with pytest.raises(process_module.AirflowFailException):
        process_module.process_aub_post()

    assert FakeEmailReader.deleted == []


def test_process_aub_post_mixed_result_deletes_only_successful_email(monkeypatch) -> None:
    _set_defaults()
    FakeEmailReader.emails = [_make_email(uid=b"3"), _make_email(uid=b"4")]

    extracted_educations = iter(["Pædagog", "Ukendt uddannelse"])

    monkeypatch.setattr(process_module.Variable, "get", lambda *args, **kwargs: _runtime_config())
    monkeypatch.setattr(process_module.BaseHook, "get_connection", lambda _id: FakeConnection())
    monkeypatch.setattr(process_module, "EmailReader", FakeEmailReader)
    monkeypatch.setattr(process_module, "EmailSender", FakeEmailSender)
    monkeypatch.setattr(
        process_module,
        "extract_education_from_pdf",
        lambda _bytes: next(extracted_educations),
    )

    with pytest.raises(process_module.AirflowFailException):
        process_module.process_aub_post()

    assert len(FakeEmailSender.sent_messages) == 1
    assert FakeEmailReader.deleted == [(b"3", "INBOX", True)]
