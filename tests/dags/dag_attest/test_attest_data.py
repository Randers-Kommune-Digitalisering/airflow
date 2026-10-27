from collections.abc import Iterator

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dag_attest.attest_data import (
    find_matching_orders,
    AttestParseError,
    extract_cpr,
    is_clean_attest,
)
from dag_attest.model import (
    AttestationOrder,
    AttestationRequest,
    Base,
    OrderStatus,
)
from dags.dag_attest.attest_data import AttestType, to_attest_type_ids


@pytest.fixture
def session() -> Iterator[Session]:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    test_session = Session(engine)

    try:
        yield test_session
    finally:
        test_session.close()
        engine.dispose()


def add_order(
        session: Session,
        *,
        subject_cpr: str,
        attest_type: int,
        status: OrderStatus,
        requester_email: str = "rekvirent@example.test",
) -> AttestationOrder:
    request = AttestationRequest(
        requester_dq="test-dq",
        requester_name="Test Rekvirent",
        requester_email=requester_email,
        subject_cpr=subject_cpr,
        is_approved=True,
    )
    order = AttestationOrder(
        request=request,
        attest_type=attest_type,
        status=status,
    )
    session.add(order)
    session.flush()
    return order


def test_find_matching_orders_returns_empty_when_nothing_matches(
        session: Session,
) -> None:
    add_order(
        session,
        subject_cpr="0101901234",
        attest_type=1,
        status=OrderStatus.ORDERED,
    )
    add_order(
        session,
        subject_cpr="0202901234",
        attest_type=1,
        status=OrderStatus.ORDERED,
    )
    add_order(
        session,
        subject_cpr="0303901234",
        attest_type=2,
        status=OrderStatus.ORDERED,
    )
    add_order(
        session,
        subject_cpr="0404901234",
        attest_type=1,
        status=OrderStatus.PENDING_ORDER,
    )
    session.flush()

    matches = find_matching_orders(
        session,
        subject_cpr="0101901234",
        attest_types={2},
    )

    assert matches == []


def test_find_matching_orders_returns_the_matching_order(
        session: Session,
) -> None:
    expected = add_order(
        session,
        subject_cpr="0101901234",
        attest_type=1,
        status=OrderStatus.ORDERED,
        requester_email="leder@example.test",
    )
    add_order(
        session,
        subject_cpr="0101901234",
        attest_type=1,
        status=OrderStatus.PENDING_ORDER,
    )
    session.flush()

    matches = find_matching_orders(
        session,
        subject_cpr="0101901234",
        attest_types={1},
    )

    assert len(matches) == 1
    assert matches[0].order_id == expected.order_id
    assert matches[0].request.requester_email == "leder@example.test"


def test_find_matching_orders_returns_all_matches_for_manual_review(
        session: Session,
) -> None:
    first = add_order(
        session,
        subject_cpr="0101901234",
        attest_type=1,
        status=OrderStatus.ORDERED,
        requester_email="foerste@example.test",
    )
    second = add_order(
        session,
        subject_cpr="0101901234",
        attest_type=1,
        status=OrderStatus.ORDERED,
        requester_email="anden@example.test",
    )
    session.flush()

    matches = find_matching_orders(
        session,
        subject_cpr="0101901234",
        attest_types={1},
    )

    assert len(matches) == 2
    assert {order.order_id for order in matches} == {
        first.order_id,
        second.order_id,
    }


def test_extract_cpr_accepts_cpr_with_and_without_dash() -> None:
    assert extract_cpr("010190-1234") == "0101901234"
    assert extract_cpr("0101901234") == "0101901234"


def test_extract_cpr_accepts_same_cpr_repeated() -> None:
    assert extract_cpr("010190-1234 ... 0101901234") == "0101901234"


def test_extract_cpr_fails_when_missing() -> None:
    with pytest.raises(AttestParseError):
        extract_cpr("Ingen cpr her")


def test_extract_cpr_fails_when_multiple_different_cprs() -> None:
    with pytest.raises(AttestParseError):
        extract_cpr("010190-1234 og 020290-5678")


def test_extract_cpr_ignores_longer_numbers() -> None:
    with pytest.raises(AttestParseError):
        extract_cpr("Journalnr. 123456789012")


def test_is_clean_attest_true_with_line_break_and_case() -> None:
    text = "Personen er IKKE registreret \n med afgørelser"
    assert is_clean_attest(text) is True


def test_is_clean_attest_false_when_phrase_missing() -> None:
    text = "Personen er registreret med afgørelser"
    assert is_clean_attest(text) is False


def test_attest_type_ids_match_from_configuration() -> None:
    assert to_attest_type_ids({AttestType.OFFENTLIG_STRAFFEATTEST}) == {0}
    assert to_attest_type_ids({AttestType.PRIVAT_STRAFFEATTEST}) == {1}
    assert to_attest_type_ids({AttestType.BOERNEATTEST}) == {2}
