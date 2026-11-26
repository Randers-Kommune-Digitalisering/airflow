from __future__ import annotations

import re
from datetime import datetime, timezone
from email.message import EmailMessage
from email.utils import parseaddr
from typing import Any


def evaluate_email_match(
    config: dict[str, Any],
    message: EmailMessage,
    flags: set[str],
    internal_date: datetime | None,
    now: datetime | None = None,
) -> tuple[bool, dict[str, bool]]:
    """
    Evaluate one email against config requirements.

    Returns (is_match, group_results) where group_results maps configured
    requirement group names to boolean results.
    """
    requirements = config.get("requirements", {})
    match_mode = config.get("match_mode", "all")

    group_results: dict[str, bool] = {}

    if "subject" in requirements:
        group_results["subject"] = _match_subject(message, requirements["subject"])

    if "from" in requirements:
        group_results["from"] = _match_from(message, requirements["from"])

    if "flags" in requirements:
        group_results["flags"] = _match_flags(flags, requirements["flags"])

    if "age" in requirements:
        group_results["age"] = _match_age(internal_date, requirements["age"], now=now)

    if "attachments" in requirements:
        group_results["attachments"] = _match_attachments(message, requirements["attachments"])

    if not group_results:
        return False, group_results

    if match_mode == "any":
        return any(group_results.values()), group_results

    return all(group_results.values()), group_results


def _match_subject(message: EmailMessage, subject_cfg: dict[str, Any]) -> bool:
    subject_text = (message.get("Subject") or "")
    lowered_subject = subject_text.lower()

    checks: list[bool] = []

    contains_any = subject_cfg.get("contains_any")
    if contains_any:
        checks.append(any(value.lower() in lowered_subject for value in contains_any))

    contains_all = subject_cfg.get("contains_all")
    if contains_all:
        checks.append(all(value.lower() in lowered_subject for value in contains_all))

    regex_patterns = subject_cfg.get("regex")
    if regex_patterns:
        checks.append(any(re.search(pattern, subject_text, flags=re.IGNORECASE) for pattern in regex_patterns))

    return all(checks) if checks else False


def _match_from(message: EmailMessage, sender_cfg: dict[str, Any]) -> bool:
    sender_address = parseaddr(message.get("From", ""))[1].strip().lower()

    checks: list[bool] = []

    allowed_senders = sender_cfg.get("match")
    if allowed_senders:
        allowed_set = {sender.lower() for sender in allowed_senders}
        checks.append(sender_address in allowed_set)

    blocked_senders = sender_cfg.get("not_match")
    if blocked_senders:
        blocked_set = {sender.lower() for sender in blocked_senders}
        checks.append(sender_address not in blocked_set)

    regex_patterns = sender_cfg.get("regex")
    if regex_patterns:
        checks.append(any(re.search(pattern, sender_address, flags=re.IGNORECASE) for pattern in regex_patterns))

    return all(checks) if checks else False


def _match_flags(flags: set[str], flags_cfg: dict[str, Any]) -> bool:
    normalized_flags = {flag.lower() for flag in flags}
    checks: list[bool] = []

    include_all = flags_cfg.get("include_all")
    if include_all:
        include_list = [flag.lower() for flag in include_all]
        checks.append(all(_flag_is_present(normalized_flags, flag) for flag in include_list))

    include_any = flags_cfg.get("include_any")
    if include_any:
        include_list = [flag.lower() for flag in include_any]
        checks.append(any(_flag_is_present(normalized_flags, flag) for flag in include_list))

    exclude_any = flags_cfg.get("exclude_any")
    if exclude_any:
        exclude_list = [flag.lower() for flag in exclude_any]
        checks.append(not any(_flag_is_present(normalized_flags, flag) for flag in exclude_list))

    exclude_all = flags_cfg.get("exclude_all")
    if exclude_all:
        exclude_list = [flag.lower() for flag in exclude_all]
        checks.append(not all(_flag_is_present(normalized_flags, flag) for flag in exclude_list))

    return all(checks) if checks else False


def _flag_is_present(normalized_flags: set[str], flag: str) -> bool:
    """
    Determine whether a requested flag is effectively present.

    IMAP servers often model unseen state by absence of \\Seen, while some tools
    expose an explicit \\Unseen pseudo-flag. Treat both representations as unseen.
    """
    if flag == "\\unseen":
        return ("\\unseen" in normalized_flags) or ("\\seen" not in normalized_flags)

    return flag in normalized_flags


def _match_age(
    internal_date: datetime | None,
    age_cfg: dict[str, Any],
    now: datetime | None = None,
) -> bool:
    if internal_date is None:
        return False

    reference_now = now or datetime.now(timezone.utc)

    if internal_date.tzinfo is None:
        internal_date = internal_date.replace(tzinfo=timezone.utc)

    age_seconds = max(0.0, (reference_now - internal_date).total_seconds())

    checks: list[bool] = []

    older_than_days = age_cfg.get("older_than_days")
    if older_than_days is not None:
        checks.append(age_seconds >= older_than_days * 86400)

    older_than_hours = age_cfg.get("older_than_hours")
    if older_than_hours is not None:
        checks.append(age_seconds >= older_than_hours * 3600)

    newer_than_days = age_cfg.get("newer_than_days")
    if newer_than_days is not None:
        checks.append(age_seconds <= newer_than_days * 86400)

    newer_than_hours = age_cfg.get("newer_than_hours")
    if newer_than_hours is not None:
        checks.append(age_seconds <= newer_than_hours * 3600)

    return all(checks) if checks else False


def _match_attachments(message: EmailMessage, attachments_cfg: dict[str, Any]) -> bool:
    attachments = list(message.iter_attachments())
    filenames = [(attachment.get_filename() or "") for attachment in attachments]
    lowered_filenames = [name.lower() for name in filenames]
    extensions = {
        name.rsplit(".", 1)[1].lower()
        for name in filenames
        if "." in name and name.rsplit(".", 1)[1]
    }

    checks: list[bool] = []

    has_attachments = attachments_cfg.get("has_attachments")
    if has_attachments is not None:
        checks.append(bool(attachments) is bool(has_attachments))

    allowed_extensions = attachments_cfg.get("type")
    if allowed_extensions:
        allowed_extension_set = {ext.lower().lstrip(".") for ext in allowed_extensions}
        checks.append(bool(extensions & allowed_extension_set))

    name_cfg = attachments_cfg.get("name") or {}
    if name_cfg:
        checks.append(_match_attachment_names(lowered_filenames, name_cfg))

    return all(checks) if checks else False


def _match_attachment_names(filenames: list[str], name_cfg: dict[str, Any]) -> bool:
    if not filenames:
        return False

    checks: list[bool] = []

    contains_any = name_cfg.get("contains_any")
    if contains_any:
        lowered_terms = [value.lower() for value in contains_any]
        checks.append(any(any(term in name for term in lowered_terms) for name in filenames))

    contains_all = name_cfg.get("contains_all")
    if contains_all:
        lowered_terms = [value.lower() for value in contains_all]
        checks.append(any(all(term in name for term in lowered_terms) for name in filenames))

    regex_patterns = name_cfg.get("regex")
    if regex_patterns:
        checks.append(any(re.search(pattern, name, flags=re.IGNORECASE) for pattern in regex_patterns for name in filenames))

    return all(checks) if checks else False
