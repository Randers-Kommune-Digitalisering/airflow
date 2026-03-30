
from email.header import decode_header
import email as email_module
from email.message import EmailMessage
from typing import TypedDict


class Attachment(TypedDict):
    filename: str | None
    content_bytes: bytes
    content_type: str
    content_disposition: str | None


def extract_attachments(mail: EmailMessage) -> list[Attachment]:
    """
    Extract attachments from an EmailMessage or raw RFC822 email.

    :param mail: An EmailMessage object or raw email content as bytes or string.
    :return: A list of attachments, where each attachment is a dictionary with keys:
      - filename: str | None
      - content_bytes: bytes
      - content_type: str
      - content_disposition: str | None
    """
    if mail is None:
        return []

    msg = mail
    if isinstance(mail, (bytes, bytearray)):
        msg = email_module.message_from_bytes(bytes(mail))
    elif isinstance(mail, str):
        msg = email_module.message_from_string(mail)

    if not hasattr(msg, "walk"):
        raise TypeError(f"Unsupported mail type for attachment extraction: {type(mail)!r}")

    attachments: list[Attachment] = []
    for part in msg.walk():
        if part.get_content_maintype() == "multipart":
            continue

        filename = part.get_filename()
        content_disposition = part.get_content_disposition()  # 'attachment', 'inline', or None

        is_attachment = content_disposition == "attachment" or bool(filename)
        if not is_attachment:
            continue

        decoded_filename = decode_mime_word(filename) if filename else None
        content_bytes = part.get_payload(decode=True) or b""
        content_type = part.get_content_type() or "application/octet-stream"

        attachments.append(
            {
                "filename": decoded_filename,
                "content_bytes": content_bytes,
                "content_type": content_type,
                "content_disposition": content_disposition,
            }
        )

    return attachments


def decode_mime_word(value: str | None) -> str | None:
    """
    Decodes a MIME-encoded word (e.g., from email headers) into a human-readable string.

    :param value: The MIME-encoded string to decode.
    :return: The decoded string, or None if the input was None.
    """
    if value is None:
        return None
    decoded_parts: list[str] = []
    for part, charset in decode_header(value):
        if isinstance(part, bytes):
            decoded_parts.append(part.decode(charset or "utf-8", errors="replace"))
        else:
            decoded_parts.append(part)
    return "".join(decoded_parts)
