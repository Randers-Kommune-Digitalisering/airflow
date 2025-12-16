import asyncio
import logging
import requests

from sqlalchemy.orm import Session

from dag_meddb_person_check.model import PersonMedDB, CommitteeMembership
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.azure.hooks.msgraph import KiotaRequestAdapterHook
from msgraph.graph_service_client import GraphServiceClient

from utils.token_provider import BearerAuth
from dag_meddb_person_check.lookups import delta_get_by_email, ms_graph_get_user_by_email_alias_async, skole_ad_get_by_email


logger = logging.getLogger(__name__)


def check_and_update_persons() -> None:
    """
    Iterate through persons in MedDB and update their information from Delta, MS Graph, and Skole-AD.
    """
    meta_hook = PostgresHook(postgres_conn_id="meta_db")
    meta_engine = meta_hook.get_sqlalchemy_engine()

    ms_graph_hook = KiotaRequestAdapterHook(conn_id="ms_graph_api")
    ms_graph_adapter = ms_graph_hook.get_conn()
    ms_graph_client = GraphServiceClient(request_adapter=ms_graph_adapter)

    delta_hook = BaseHook.get_connection('delta_prod')

    delta_session = requests.Session()
    delta_session.auth = BearerAuth(
        token_url=delta_hook.extra_dejson.get('token_url'),
        client_id=delta_hook.login,
        client_secret=delta_hook.password
    )

    async def _process_all():
        with Session(bind=meta_engine) as meta_session:
            persons = meta_session.query(PersonMedDB).all()
            total = len(persons)
            logger.info(f"Found {total} persons to check.")
            progress_step = max(1, total // 20)
            for idx, person in enumerate(persons):
                if total > 0 and idx % progress_step == 0:
                    percent = (idx / total) * 100
                    logger.info(f"Progress: {percent:.1f}% ({idx}/{total})")
                if person.email:
                    user = delta_get_by_email(session=delta_session, base_url=delta_hook.host, email=person.email)
                    if not user:
                        user = skole_ad_get_by_email(session=meta_session, email=person.email)
                        if not user:
                            user = await ms_graph_get_user_by_email_alias_async(client=ms_graph_client, email_alias=person.email)

                    if user:
                        email = user.get("email", None)
                        if email:
                            duplicate = meta_session.query(PersonMedDB).filter(
                                PersonMedDB.email == email,
                                PersonMedDB.id != person.id
                            ).first()
                            if duplicate:
                                meta_session.query(CommitteeMembership).filter(
                                    CommitteeMembership.person_id == duplicate.id
                                ).update({CommitteeMembership.person_id: person.id})
                                meta_session.delete(duplicate)
                                meta_session.flush()

                        person.email = email or person.email
                        person.name = user.get("name", None) or person.name
                        person.organization = user.get("unit", None) or person.organization
                        person.username = user.get("username", None) or person.username
                        person.found_in_system = True

            meta_session.commit()

    asyncio.run(_process_all())
