from __future__ import annotations

from datetime import timezone

from dag_mailbox_cleaner.imap_client import (
    _collect_fetch_metadata_bytes,
    _parse_flags_from_fetch_metadata,
    _parse_internal_date_from_fetch_metadata,
)


def test_collect_fetch_metadata_bytes_handles_split_fetch_response() -> None:
    fetch_data = [
        (b"1 (RFC822 {123}", b"raw-message"),
        b" FLAGS (\\Seen) INTERNALDATE \"23-Jul-2026 08:20:00 +0000\")",
    ]

    metadata = _collect_fetch_metadata_bytes(fetch_data)

    assert b"FLAGS" in metadata
    assert b"INTERNALDATE" in metadata


def test_parse_flags_and_internal_date_from_collected_metadata() -> None:
    fetch_data = [
        (b"1 (RFC822 {123}", b"raw-message"),
        b" UID 450 FLAGS (\\Seen \\Flagged) INTERNALDATE \"23-Jul-2026 08:20:00 +0000\")",
    ]

    metadata = _collect_fetch_metadata_bytes(fetch_data)

    flags = _parse_flags_from_fetch_metadata(metadata)
    internal_date = _parse_internal_date_from_fetch_metadata(metadata)

    assert "\\Seen" in flags
    assert "\\Flagged" in flags
    assert internal_date is not None
    assert internal_date.tzinfo == timezone.utc
    assert internal_date.year == 2026
