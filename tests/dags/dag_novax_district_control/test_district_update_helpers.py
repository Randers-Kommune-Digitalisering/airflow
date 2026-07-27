from datetime import date, datetime
from types import SimpleNamespace

import pytest

from dag_novax_district_control.district_update_helpers import (
    _addresses_are_equivalent,
    _parse_novax_address,
    update_address_from_dataforsyning,
)


def test_parse_novax_address_extracts_all_components() -> None:
    parsed = _parse_novax_address("John Doe Vej 6, 3. 65, 8930 Randers NØ")

    assert parsed == {
        "street_name": "john doe vej",
        "house_number": "6",
        "floor": "3",
        "door": "65",
        "locality": "",
        "postal_code": "8930",
        "city": "randers nø",
    }


def test_addresses_are_equivalent_across_novax_variants() -> None:
    lhs = "John Doe Vej 6, 3.,-65, 8930 Randers NØ"
    rhs = "John Doe Vej 6, 3. 65, 8930 Randers NØ"

    assert _addresses_are_equivalent(lhs, rhs) is True


@pytest.mark.parametrize(
    ("lhs", "rhs"),
    [
        (
            "Alpha Road 72 Hamlet 8930 Test City",
            "Alpha Road 72, Hamlet, 8930 Test City",
        ),
        (
            "Beta Street 4B, 1. 10, 8990 North Town",
            "Beta Street 4 B,1,-10, 8990 North Town",
        ),
        (
            "Gamma Lane 14, 3. tv 8900 Central City",
            "Gamma Lane 14, 3. tv, 8900 Central City",
        ),
        (
            "Delta Avenue 1,st tv 8900 Central City",
            "Delta Avenue 1, st. tv, 8900 Central City",
        ),
        (
            "Epsilon Drive 2 C 0020 West World",
            "Epsilon Drive 2C, 0020 West World",
        ),
    ],
)
def test_addresses_are_equivalent_for_known_novax_log_patterns(lhs: str, rhs: str) -> None:
    assert _addresses_are_equivalent(lhs, rhs) is True


def test_update_address_skips_change_when_only_format_differs() -> None:
    entry = SimpleNamespace(
        ID="name-1",
        ADRESSE="John Doe Vej 6, 3.,-65, 8930 Randers NØ",
        addresses=[],
    )

    changed = update_address_from_dataforsyning(
        entry=entry,
        address_info={"full_address": "John Doe Vej 6, 3. 65, 8930 Randers NØ"},
        reference_date=date(2026, 1, 1),
        close_to_dt=date(2026, 1, 1),
        new_from_dt=date(2026, 1, 1),
        now_dt=datetime(2026, 1, 1, 12, 0, 0),
        now_time="12:00",
    )

    assert changed is False
    assert entry.ADRESSE == "John Doe Vej 6, 3.,-65, 8930 Randers NØ"
