from email.message import EmailMessage

import pytest

from dag_aub_post.aub_post_data import (
    build_education_contact_map,
    _extract_education_from_text,
    find_attachment_by_name,
)


def test_build_education_contact_map_allows_multiple_educations_per_contact() -> None:
    mapping = build_education_contact_map(
        [
            {
                "email": "kontakt1@randers.dk",
                "educations": ["Social- og sundhedshjælper", "Social- og sundhedsassistent"],
            },
            {
                "email": "kontakt2@randers.dk",
                "educations": ["Pædagog"],
            },
        ]
    )

    assert mapping["social- og sundhedshjælper"] == "kontakt1@randers.dk"
    assert mapping["social- og sundhedsassistent"] == "kontakt1@randers.dk"
    assert mapping["pædagog"] == "kontakt2@randers.dk"


def test_build_education_contact_map_rejects_duplicate_education_across_contacts() -> None:
    with pytest.raises(ValueError, match="assigned to multiple contacts"):
        build_education_contact_map(
            [
                {
                    "email": "kontakt1@randers.dk",
                    "educations": ["Pædagog"],
                },
                {
                    "email": "kontakt2@randers.dk",
                    "educations": ["Pædagog"],
                },
            ]
        )


def test__extract_education_from_text_matches_expected_regex() -> None:
    text = "Header\nUddannelse\nSocial- og sundhedsassistent\nFooter"
    assert _extract_education_from_text(text) == "Social- og sundhedsassistent"


def test__extract_education_from_text_raises_when_regex_not_found() -> None:
    with pytest.raises(ValueError, match="Could not find education"):
        _extract_education_from_text("Ingen relevant tekst")


def test_find_attachment_by_name_is_case_insensitive() -> None:
    message = EmailMessage()
    message["Subject"] = "AUB"
    message.set_content("mail body")
    message.add_attachment(
        b"fake-pdf-content",
        maintype="application",
        subtype="pdf",
        filename="MainDoc.PDF",
    )

    filename, payload = find_attachment_by_name(message, target_filename="maindoc.pdf")

    assert filename == "MainDoc.PDF"
    assert payload == b"fake-pdf-content"
