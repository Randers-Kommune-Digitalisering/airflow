# Mailbox Cleaner Config Guide

This guide explains how to configure a cleanup job for the Mailbox Cleaner DAG.

## Purpose

A cleanup job is defined by a JSON object configuration. Each configuration:

- Targets one mailbox and one Airflow mail connection.
- Defines one or more matching requirements.
- Defines one action to run on matching emails.
- Applies safety limits for safe operation.

## Config Storage

Store each configuration in a separate Airflow Variable.

- Use the naming pattern `mailbox_cleaner_conf_<job_name>`.
- `<job_name>` should be unique and descriptive.
- Prefer using the same value as the config `id` to keep naming consistent.
- Each Airflow Variable should contain exactly one config JSON object.
- Each job is intended to run as an individual task.

## Config Shape

```json
{
  "id": "mailbox_cleanup",
  "enabled": true,
  "mail_connection_id": "mailbox_cleaner_demo_imap",
  "mailbox": "INBOX",
  "match_mode": "all",
  "requirements": {},
  "action": {},
  "safety": {}
}
```

## Top-Level Properties

- `id` (string, required): Unique rule id.
- `description` (string, optional): Rule description/notes.
- `enabled` (boolean, optional, default `true`): Disable a job without deleting it.
- `mail_connection_id` (string, required): Airflow connection id for IMAP credentials.
- `mailbox` (string, optional, default `INBOX`): Mailbox/folder to scan.
- `match_mode` (string, optional, default `all`):
  - `all`: All configured requirement groups must match.
  - `any`: At least one configured requirement group must match.
- `requirements` (object, required): Filters used to match emails.
- `action` (object, required): What to do with matched emails.
- `safety` (object, optional): Runtime guardrails.

## Requirements

At least one requirement must be configured.

### `requirements.subject`

- `contains_any` (string[]): Match if subject contains any value.
- `contains_all` (string[]): Match if subject contains all values.
- `regex` (string[]): Match if subject matches any regex pattern.

### `requirements.from`

- `match` (string[]): Allowed sender addresses.
- `not_match` (string[]): Blocked sender addresses.
- `regex` (string[]): Allowed sender regex patterns.

### `requirements.flags`

- `include_all` (string[]): All flags must be present.
- `include_any` (string[]): At least one flag must be present.
- `exclude_any` (string[]): None of these flags may be present.
- `exclude_all` (string[]): Not all of these flags may be present together.

### `requirements.age`

- `older_than_days` (int): Message age must be older than this many days.
- `older_than_hours` (int): Same as above, in hours.
- `newer_than_days` (int): Message age must be newer than this many days.
- `newer_than_hours` (int): Same as above, in hours.

### `requirements.attachments`

- `has_attachments` (boolean): Require with/without attachments.
- `type` (string[]): Allowed attachment file extensions (for example `pdf`, `docx`).
- `name.contains_any` (string[]): Match if filename contains any value.
- `name.contains_all` (string[]): Match if filename contains all values.
- `name.regex` (string[]): Match if filename matches any regex pattern.

## Action

- `action.type` (string, required): `delete` or `move`.
- `action.target_mailbox` (string, required when `type = move`): Destination mailbox/folder.

## Safety

- `safety.dry_run` (boolean, optional, default `true`): Log what would happen, but do not change emails.
- `safety.max_messages_per_run` (int, optional): Hard cap per run.
- `safety.min_age_for_delete_days` (int, optional): Extra protection for delete action.

## Validation Rules

Use these rules when implementing validation:

- At least one requirement must be set.
- In `subject`, do not combine `contains_any` and `regex` unless you explicitly support that behavior.
- In `from`, do not combine `match` and `regex` unless you explicitly support that behavior.
- In `age`, use only one unit per direction:
  - `older_than_days` XOR `older_than_hours`
  - `newer_than_days` XOR `newer_than_hours`
- If `action.type = move`, `target_mailbox` is required.
- If `action.type = delete`, enforce `min_age_for_delete_days`.
- Reject unknown properties to avoid silent config mistakes.

## Minimal Example

```json
{
  "id": "move_old_invoices",
  "enabled": true,
  "mail_connection_id": "mailbox_cleaner_imap",
  "mailbox": "INBOX",
  "match_mode": "all",
  "requirements": {
    "subject": {
      "contains_any": ["Invoice"]
    },
    "age": {
      "older_than_days": 30
    }
  },
  "action": {
    "type": "move",
    "target_mailbox": "Archive"
  },
  "safety": {
    "dry_run": true,
    "max_messages_per_run": 100
  }
}
```

## Full Example

```json
{
  "id": "invoice_reminders_cleanup",
  "enabled": true,
  "mail_connection_id": "mailbox_cleaner_demo_imap",
  "mailbox": "INBOX",
  "match_mode": "all",
  "requirements": {
    "subject": {
      "contains_any": ["Invoice", "Payment"],
      "contains_all": ["Reminder"]
    },
    "from": {
      "not_match": ["noreply@example.com"],
      "regex": [".*@example\\.com"]
    },
    "flags": {
      "include_all": ["\\Seen"]
    },
    "age": {
      "older_than_days": 30,
      "newer_than_days": 365
    },
    "attachments": {
      "has_attachments": true,
      "type": ["pdf", "docx"],
      "name": {
        "contains_any": ["invoice", "payment"]
      }
    }
  },
  "action": {
    "type": "move",
    "target_mailbox": "Archive"
  },
  "safety": {
    "dry_run": true,
    "max_messages_per_run": 200,
    "min_age_for_delete_days": 14
  }
}
```

## Recommended Rollout

- Start with `dry_run: true`.
- Verify logs and expected hit count.
- Enable non-destructive actions first (`move` to an archive mailbox/folder).
- Enable `delete` only after validation and retention agreement.
