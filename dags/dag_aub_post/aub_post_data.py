import re
from email.message import Message


EDUCATION_REGEX = re.compile(r"Uddannelse\s*\n(.*)", re.IGNORECASE)


def normalize_education(value: str) -> str:
    """
    Normalize the education value by removing extra whitespace and converting to lowercase.

    :param value: The original education string.
    :return: The normalized education string.
    """
    return " ".join(value.split()).casefold()


def build_education_contact_map(contact_mappings: list[dict]) -> dict[str, str]:
    """
    Build a mapping from normalized education names to contact emails based on the provided configuration.

    :param contact_mappings: A list of dictionaries, each containing an "email" key and an "educations" key (list of education names).
    :return: A dictionary mapping normalized education names to contact emails.
    """
    if not isinstance(contact_mappings, list) or not contact_mappings:
        raise ValueError("'contact_mappings' must be a non-empty list")

    contact_map: dict[str, str] = {}
    for entry in contact_mappings:
        if not isinstance(entry, dict):
            raise ValueError("Each contact mapping must be a JSON object")

        email = entry.get("email")
        educations = entry.get("educations")

        if not isinstance(email, str) or not email.strip():
            raise ValueError("Each contact mapping must have a non-empty 'email'")

        if not isinstance(educations, list) or not educations:
            raise ValueError("Each contact mapping must have a non-empty 'educations' list")

        resolved_email = email.strip()
        for education in educations:
            if not isinstance(education, str) or not education.strip():
                raise ValueError("Each education value must be a non-empty string")

            education_key = normalize_education(education)
            previous_email = contact_map.get(education_key)
            if previous_email and previous_email != resolved_email:
                raise ValueError(
                    f"Education '{education}' is assigned to multiple contacts"
                )
            contact_map[education_key] = resolved_email

    return contact_map


def extract_education_from_text(text: str) -> str:
    """
    Extract the education name from the provided text using a regex pattern.

    :param text: The text content extracted from the PDF.
    :return: The extracted education name.
    """
    if not isinstance(text, str) or not text.strip():
        raise ValueError("No text content was available for education extraction")

    match = EDUCATION_REGEX.search(text)
    if not match:
        raise ValueError("Could not find education in PDF text using the expected regex")

    education = match.group(1).strip()
    if not education:
        raise ValueError("Education field was found but empty")

    return education


def extract_education_from_pdf(pdf_bytes: bytes) -> str:
    """
    Extract the education name from the PDF bytes by first extracting text and then applying the education regex.

    :param pdf_bytes: The raw bytes of the PDF file.
    :return: The extracted education name.
    """
    if not pdf_bytes:
        raise ValueError("Attachment bytes are empty")

    import fitz

    with fitz.open(stream=pdf_bytes, filetype="pdf") as document:
        pdf_text = "\n".join(page.get_text() for page in document)

    return extract_education_from_text(pdf_text)


def find_attachment_by_name(message: Message, target_filename: str = "maindoc.pdf") -> tuple[str, bytes]:
    """
    Find an attachment in the email message by its filename.

    :param message: The email message object.
    :param target_filename: The name of the attachment to find.
    :return: A tuple containing the filename and the attachment bytes.
    """
    if not target_filename.strip():
        raise ValueError("target_filename must be a non-empty string")

    target_name = target_filename.strip().casefold()
    for part in message.walk():
        if part.get_content_disposition() != "attachment":
            continue

        filename = part.get_filename()
        if not filename:
            continue

        if filename.strip().casefold() != target_name:
            continue

        payload = part.get_payload(decode=True)
        if payload is None:
            raise ValueError(f"Attachment '{filename}' has no decodable payload")

        return filename, payload

    raise ValueError(f"Attachment '{target_filename}' was not found")


def resolve_contact_email(education: str, education_contact_map: dict[str, str]) -> str:
    """
    Resolve the contact email for the given education using the provided education-contact map.

    :param education: The name of the education.
    :param education_contact_map: A dictionary mapping normalized education names to contact emails.
    :return: The resolved contact email.
    """
    resolved_email = education_contact_map.get(normalize_education(education))
    if not resolved_email:
        raise ValueError("No contact email configured for extracted education")
    return resolved_email
