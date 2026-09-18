from email.header import Header, decode_header, make_header


def build_safe_subject_header(raw_subject: object) -> str:
    """
    Build an ASCII-safe RFC 2047 subject header.

    Some incoming subjects are already decoded to Unicode by the IMAP parser.
    Re-encoding here prevents downstream MIME serialization from failing on
    characters like "æ".

    :param raw_subject: Raw subject value from the email message.
    :return: ASCII-safe subject header, or an empty string when no subject exists.
    """
    if raw_subject is None:
        return ""

    try:
        subject_text = str(make_header(decode_header(str(raw_subject)))).strip()
    except Exception:
        subject_text = str(raw_subject).strip()

    if not subject_text:
        return ""

    if subject_text.isascii():
        return subject_text

    return Header(subject_text, "utf-8").encode()


def _decode_email_text(payload: bytes, declared_charset: str | None) -> str:
    """
    Decode message bytes using declared charset with fallbacks that handle
    Danish characters commonly found in legacy encodings.

    :param payload: Raw message body bytes.
    :param declared_charset: Charset declared by the email message, if any.
    :return: Decoded message body text.
    """
    if not payload:
        return ""

    candidate_charsets = [declared_charset, "utf-8", "cp1252", "iso-8859-1"]
    tried: set[str] = set()

    for charset in candidate_charsets:
        if not charset:
            continue
        normalized = charset.strip().lower()
        if not normalized or normalized in tried:
            continue
        tried.add(normalized)
        try:
            return payload.decode(normalized)
        except (LookupError, UnicodeDecodeError):
            continue

    return payload.decode("utf-8", errors="replace")


def get_message_body(message) -> str:
    """
    Return the original plain-text message body without attachment data.

    :param message: Parsed email message.
    :return: Message body text, or an empty string when no body is available.
    """
    if message.is_multipart():
        body_part = message.get_body(preferencelist=("plain", "html"))
        if body_part is None:
            return ""
        payload = body_part.get_payload(decode=True)
        declared_charset = body_part.get_content_charset()
    else:
        payload = message.get_payload(decode=True)
        declared_charset = message.get_content_charset()

    if isinstance(payload, bytes):
        return _decode_email_text(payload, declared_charset)
    return str(payload or "")
