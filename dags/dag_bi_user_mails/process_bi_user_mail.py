import logging
from io import BytesIO
import pandas as pd

from airflow.models import Variable
from airflow.providers.sftp.hooks.sftp import SFTPHook

from dag_bi_user_mails.model import BiMailUser
from rkdigi.email_handling import EmailSender
from rkdigi.database_manager import DatabaseManager
from dag_bi_user_mails.bi_user_mails_data import (
    get_user_by_email,
    add_user,
    mark_email_sent,
    send_mail
)

logger = logging.getLogger(__name__)

def process_bi_user_mail() -> None:
    """
    Process bi_user_mail data.
    """
    logger.info("Starting to process bi_user_mail data...")

    try:
        bi_user_mail_runtime_config = Variable.get("bi_user_mail_runtime_config", deserialize_json=True)
    except Exception as e:
        logger.error("Error retrieving bi_user_mail_runtime_config: %s", e)
        raise

    try:

        sftp_hook = SFTPHook(ssh_conn_id="intftp_kmd")

        with sftp_hook.get_conn() as sftp_client:

            with sftp_client.open(filename=bi_user_mail_runtime_config["sftp_file_path"], mode="r") as sftp_file:

                # As xlsx files are compressed we need to read the file into a BytesIO buffer to enable seek()
                file_buffer = BytesIO(sftp_file.read())

                # Map columns and parse into dict
                data_frame = pd.read_excel(file_buffer, engine="openpyxl", header=2, usecols="B:H")
                data_frame = data_frame.dropna(how="all")
                data_frame.columns = [col.strip().replace(' ', '_').replace('-', '_').lower() for col in data_frame.columns]  # Convert columns to snake_case
                records = data_frame.to_dict(orient="records")

    except Exception as e:
        logger.error("Error retrieving or processing the Excel file from sftp: %s", e)
        raise

    db_manager = DatabaseManager(
        profile_name="bi_user_mail_db",
        db_type="postgres",
        airflow_connection_id="bi_user_mail_db",
        base_model=BiMailUser
    )
    db_session = db_manager.get_session()

    logger.info("DatabaseManager initialized for profile: bi_user_mail_db")

    smtp_server = bi_user_mail_runtime_config["smtp_server"]
    email_sender = EmailSender(smtp_server=smtp_server)

    for record in records:

        user_existing = get_user_by_email(db_session, record["email_adresse"])
        user_notified = False

        if user_existing is None:
            user_data = {
                "creation_date": record.get("oprettelsesdato"),
                "name": record.get("bruger_navn"),
                "dq": record.get("bruger_id"),
                "email": record.get("email_adresse"),
                "user_group": record.get("bruger_gruppe_navn"),
                "email_sent": False,
                "email_sent_date": None
            }
            add_user(db_session, user_data)

        if user_notified or (user_existing and user_existing.email_sent):
            continue  # Skip sending email if the user has already been notified

        else:
            send_mail(email_sender, record)
            mark_email_sent(db_session, record["email_adresse"])

            user_notified = True

    db_session.close()

    logger.info("Finished processing bi_user_mail data.")
