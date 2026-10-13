from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dag_bi_user_mails.bi_user_mails_data import add_user, get_user_by_email, mark_email_sent
from dag_bi_user_mails.model import Base, BiMailUser


def test_email_lookup_matches_legacy_addresses_and_stores_normalized_email() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        session.add(BiMailUser(
            name="Duplicate", dq="duplicate", email="legacy@example.dk",
            user_group="test", email_sent=False, creation_date=datetime(2026, 1, 1),
        ))
        legacy_user = BiMailUser(
            name="Legacy", dq="legacy", email="  Legacy@Example.DK  ",
            user_group="test", email_sent=True, creation_date=datetime(2026, 1, 1),
        )
        session.add(legacy_user)
        session.commit()

        assert get_user_by_email(session, " legacy@example.dk ") == legacy_user

        new_user = add_user(session, {
            "name": "New", "dq": "new", "email": "  New@Example.DK  ",
            "user_group": "test", "email_sent": False,
            "creation_date": datetime(2026, 1, 1),
        })
        assert new_user.email == "new@example.dk"
        mark_email_sent(session, " NEW@example.dk ")
        assert new_user.email_sent is True
