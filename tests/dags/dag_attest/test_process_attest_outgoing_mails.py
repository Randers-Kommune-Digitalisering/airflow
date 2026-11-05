import pytest

from dag_attest.model import NotificationType
from dag_attest.process_attest_outgoing_mails import (
    _HR_MAILBOX,
    _notification_details,
)


@pytest.mark.parametrize(
    (
        "notification_type",
        "is_clean",
        "expected_recipient",
        "expected_text",
        "expected_attachment",
    ),
    [
        (
            NotificationType.RECORD_JOURNALIZED,
            True,
            "rekvirent@example.test",
            "uden registrerede forhold",
            True,
        ),
        (
            NotificationType.RECORD_JOURNALIZED,
            False,
            "rekvirent@example.test",
            "Personale og HR følger op",
            False,
        ),
        (
            NotificationType.RECORD_ATTACHED,
            True,
            "rekvirent@example.test",
            "manuel journalisering",
            True,
        ),
        (
            NotificationType.RECORD_UNDER_REVIEW,
            True,
            "rekvirent@example.test",
            "sendt til Personale og HR",
            False,
        ),
    ],
)
def test_notification_details(
    notification_type,
    is_clean,
    expected_recipient,
    expected_text,
    expected_attachment,
)-> None:
    recipient, body, attach_pdf = _notification_details(
        notification_type,
        is_clean,
        "rekvirent@example.test",
        "Mette Larsen",
        "Offentlig straffeattest",
    )
    assert recipient == expected_recipient
    assert expected_text in body
    assert attach_pdf == expected_attachment


def test_record_requires_review_includes_requester_details_for_hr() -> None:
    recipient, body, attach_pdf = _notification_details(
        NotificationType.RECORD_REQUIRES_REVIEW,
        False,
        "mette@example.test",
        "Mette Larsen",
        "Offentlig straffeattest",
    )
    assert recipient == _HR_MAILBOX
    assert "Rekvirent: Mette Larsen (mette@example.test)" in body
    assert attach_pdf is True


def test_notification_details_rejects_unsupported_type() -> None:
    with pytest.raises(ValueError, match="Unknown notification type"):
        _notification_details(
            NotificationType.ORDER_CONFIRMATION,
            True,
            "rekvirent@example.test",
            "Mette Larsen",
            "Offentlig straffeattest"
        )