import logging
from sqlalchemy.orm import Session
import pandas as pd

from airflow.models import Variable

from dag_bi_user_mails.model import BiMailUser
from rkdigi.email_handling import EmailSender

logger = logging.getLogger(__name__)

def _get_all_users(conn: Session) -> list[BiMailUser]:
    """
    Retrieve all BiMailUser records from the database.

    Args:
        conn: SQLAlchemy connection object.
    Returns:
        List[BiMailUser]: A list of all user records.
    """
    return conn.query(BiMailUser).all()

def get_user_by_email(conn: Session, email: str):
    """
    Retrieve a BiMailUser record by email from the database.

    Args:
        conn: SQLAlchemy connection object.
        email (str): The email address to search for.
    Returns:
        BiMailUser: The user record matching the email, or None if not found.
    """
    return conn.query(BiMailUser).filter(BiMailUser.email == email).first()

def add_user(conn: Session, user_data: dict) -> BiMailUser:
    """
    Add a new BiMailUser record to the database.

    Args:
        conn: SQLAlchemy connection object.
        user_data (dict): A dictionary containing user data.
    Returns:
        BiMailUser: The newly created user record.
    """
    new_user = BiMailUser(**user_data)
    conn.add(new_user)
    conn.commit()

    return new_user

def mark_email_sent(conn: Session, email: str) -> None:
    """
    Mark the email as sent for a BiMailUser record in the database.

    Args:
        conn: SQLAlchemy connection object.
        email (str): The email address of the user to update.
    """
    user = get_user_by_email(conn, email)
    if user:
        user.email_sent = True
        user.email_sent_date = pd.Timestamp.now()
        conn.commit()

def _build_welcome_email(runtime_config: dict, user_data: dict) -> tuple[str, str]:
    """
    Build the subject and body of the welcome email for a new user.

    Args:
        user_data (dict): A dictionary containing user data.
        runtime_config (dict): A dictionary containing runtime configuration.
    Returns:
        tuple[str, str]: A tuple containing the email subject and body.
    """

    bi_contacts = runtime_config.get("bi_contact_list", "").strip()
    subject = "Velkommen til BI"
    body = f"""
Hej {user_data['bruger_navn']},

IT har netop godkendt din BI-licens.

For at komme ind på Randers Kommunes BI-portal, skal du have installeret en genvej. Du kan finde en vejledning her:
https://broen.randers.dk/media/2ztnkdd4/vejledning-til-installation-af-genvej-til-randers-kommunes-bi-portal.pdf

Giv lyd hvis der er udfordringer. BI-kontaktpersonerne er:
{bi_contacts}

Venlig hilsen IT
    """
    body = body.strip()

    return subject, body

def _build_notification_email(runtime_config: dict, user_data: dict) -> tuple[str, str]:
    """
    Build the subject and body of the notification email for a new user.

    Args:
        runtime_config (dict): A dictionary containing runtime configuration.
        user_data (dict): A dictionary containing user data.
    Returns:
        tuple[str, str]: A tuple containing the email subject and body.
    """

    subject = f"Ny BI-udviklerlicens til {user_data['bruger_navn']}"
    body = f"""
OBS:

Der er netop oprettet en ny BI-udviklerlicens til {user_data['bruger_navn']} <{user_data['email_adresse']}>.

Venlig hilsen IT
    """
    body = body.strip()

    return subject, body

def send_mail(email_sender: EmailSender, user_data: dict) -> None:
    """
    Send an email notification for a new user.

    Args:
        email_sender (EmailSender): An instance of the EmailSender class.
        user_data (dict): A dictionary containing user data.
    """

    bi_user_mail_runtime_config = Variable.get("bi_user_mail_runtime_config", deserialize_json=True)

    user_email_subject, user_email_body = _build_welcome_email(bi_user_mail_runtime_config, user_data)

    email_sender.send_email(
        sender=bi_user_mail_runtime_config["sender_email"],
        recipients=["mikkel.bach.skaerris@randers.dk"],
        subject=user_email_subject,
        body=user_email_body,
        attachments=[],
    )

    logger.info(f"Sent welcome email to: {user_data['email_adresse']}")

    if user_data.get("bruger_gruppe_navn") != bi_user_mail_runtime_config.get("default_user_group", "Web statistik bruger").strip():

        notification_subject, notification_body = _build_notification_email(bi_user_mail_runtime_config, user_data)

        email_sender.send_email(
            sender=bi_user_mail_runtime_config["sender_email"],
            recipients=["mikkel.bach.skaerris@randers.dk"],
            subject=notification_subject,
            body=notification_body,
            attachments=[],
        )

        logger.info(f"Sent notification email for new user: {user_data['email_adresse']}")

