import logging
from typing import Any

from dag_mailbox_cleaner.config_validation import validate_config
from dag_mailbox_cleaner.imap_client import ImapClient
from dag_mailbox_cleaner.mail_matching import evaluate_email_match

from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException, AirflowFailException

logger = logging.getLogger(__name__)

DEFAULT_MAILBOX = "INBOX"
DEFAULT_SEARCH_CRITERIA = "ALL"


def process_mailbox_cleaner(config: dict[str, Any] | None = None) -> None:
    """
    Process one mailbox-cleaner job configuration.
    """
    job_config = config or {}
    config_id = str(job_config.get("id", "unknown"))

    logger.info("Validating mailbox_cleaner configuration for id=%s", config_id)

    is_valid, error_message = validate_config(job_config)
    if not is_valid:
        raise AirflowFailException(f"Configuration validation failed: {error_message}")

    if not job_config.get("enabled", True):
        logger.info("Skipping disabled mailbox cleaner config id=%s", config_id)
        return

    mailbox = str(job_config.get("mailbox", DEFAULT_MAILBOX))
    requirements = job_config.get("requirements", {})
    safety = job_config.get("safety", {})
    action = job_config.get("action", {})
    dry_run = bool(safety.get("dry_run", True))
    max_messages = safety.get("max_messages_per_run")

    if max_messages is not None:
        max_messages = int(max_messages)

    airflow_connection_id = str(job_config["mail_connection_id"])
    connection = BaseHook.get_connection(airflow_connection_id)

    imap_port = 143
    if connection.port:
        try:
            imap_port = int(connection.port)
        except (TypeError, ValueError) as exc:
            raise AirflowFailException(
                f"Airflow connection '{airflow_connection_id}' port must be an integer"
            ) from exc

    logger.info(
        "Starting mailbox clean id=%s mailbox=%s action=%s dry_run=%s",
        config_id,
        mailbox,
        action.get("type"),
        dry_run,
    )

    matched_uids: list[bytes] = []
    fetch_failures: list[str] = []

    with ImapClient(
        host=connection.host,
        port=imap_port,
        username=connection.login,
        password=connection.password,
    ) as imap_client:
        imap_client.select_mailbox(mailbox=mailbox)

        candidate_uids = imap_client.search_uids(
            criteria=DEFAULT_SEARCH_CRITERIA,
            max_results=max_messages,
            newest_first=True,
        )

        logger.info(
            "Candidate email count for id=%s mailbox=%s is %d",
            config_id,
            mailbox,
            len(candidate_uids),
        )

        if not candidate_uids:
            logger.info("No candidate emails found for id=%s", config_id)
            return

        # Phase 1: collect matching UIDs before mutating mailbox state.
        for uid in candidate_uids:
            uid_text = _uid_to_text(uid)

            try:
                fetched = imap_client.fetch_message(uid)
            except Exception as exc:  # pragma: no cover - defensive boundary
                fetch_failures.append(f"uid={uid_text}: {exc}")
                logger.warning("Failed to fetch email uid=%s for id=%s: %s", uid_text, config_id, exc)
                continue

            is_match, group_results = evaluate_email_match(
                config={
                    "requirements": requirements,
                    "match_mode": job_config.get("match_mode", "all"),
                },
                message=fetched.message,
                flags=fetched.flags,
                internal_date=fetched.internal_date,
            )

            logger.debug(
                "Evaluated uid=%s id=%s is_match=%s groups=%s",
                uid_text,
                config_id,
                is_match,
                group_results,
            )

            if is_match:
                matched_uids.append(uid)
            else:
                logger.debug(
                    "Candidate did not match id=%s uid=%s group_results=%s flags=%s internal_date=%s",
                    config_id,
                    uid_text,
                    group_results,
                    sorted(fetched.flags),
                    fetched.internal_date,
                )

        logger.info(
            "Evaluation summary for id=%s: candidates=%d matched=%d fetch_failures=%d",
            config_id,
            len(candidate_uids),
            len(matched_uids),
            len(fetch_failures),
        )

        if not matched_uids:
            if fetch_failures:
                raise AirflowException(
                    f"Mailbox cleaner id={config_id} found no matching emails and had "
                    f"{len(fetch_failures)} fetch failure(s)."
                )
            return

        action_type = str(action.get("type", ""))
        target_mailbox = action.get("target_mailbox")

        if action_type == "move" and not target_mailbox:
            raise AirflowFailException(
                "Action requires 'target_mailbox' when action.type is 'move'"
            )

        if dry_run:
            logger.info(
                "Dry-run enabled for id=%s. Planned %d action(s): action=%s mailbox=%s target=%s",
                config_id,
                len(matched_uids),
                action_type,
                mailbox,
                target_mailbox,
            )
            return

        # Phase 2: execute actions for pre-collected UIDs.
        action_failures: list[str] = []
        acted_count = 0

        for uid in matched_uids:
            uid_text = _uid_to_text(uid)
            try:
                if action_type == "move":
                    imap_client.move_email(uid=uid, target_mailbox=str(target_mailbox))
                elif action_type == "delete":
                    imap_client.delete_email(uid=uid)
                else:
                    raise ValueError(f"Unsupported action type: {action_type}")
                acted_count += 1
            except Exception as exc:  # pragma: no cover - defensive boundary
                action_failures.append(f"uid={uid_text}: {exc}")
                logger.error("Failed action for id=%s uid=%s action=%s: %s", config_id, uid_text, action_type, exc)

        try:
            imap_client.expunge_if_needed()
        except Exception as exc:  # pragma: no cover - defensive boundary
            action_failures.append(f"expunge: {exc}")
            logger.error("Failed expunge for id=%s mailbox=%s: %s", config_id, mailbox, exc)

    if fetch_failures or action_failures:
        raise AirflowException(
            f"Mailbox cleaner id={config_id} completed with failures. "
            f"fetch_failures={len(fetch_failures)} action_failures={len(action_failures)} acted={acted_count}"
        )

    logger.info(
        "Mailbox cleaner completed id=%s candidates=%d matched=%d acted=%d",
        config_id,
        len(candidate_uids),
        len(matched_uids),
        acted_count,
    )


def _uid_to_text(uid: bytes) -> str:
    return uid.decode(errors="ignore")
