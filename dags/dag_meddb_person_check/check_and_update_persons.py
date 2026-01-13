import asyncio
import logging
import httpx

from sqlalchemy.orm import Session

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.azure.hooks.msgraph import KiotaRequestAdapterHook
from msgraph.graph_service_client import GraphServiceClient

from utils.token_provider import AsyncOAuth2TokenProvider
from dag_meddb_person_check.model import PersonMedDB
from dag_meddb_person_check.lookups import delta_get_by_email, ms_graph_get_user_by_email_alias, skole_ad_get_by_email


logger = logging.getLogger(__name__)
CONCURRENCY = 10


def check_and_update_persons() -> None:
    """
    Iterate through persons in MedDB and update their information from Delta, MS Graph, and Skole-AD.
    """
    meta_hook = PostgresHook(postgres_conn_id="meta_db")
    meta_engine = meta_hook.get_sqlalchemy_engine()

    meta_conn = BaseHook.get_connection("meta_db")
    meta_uri = meta_conn.get_uri()
    if meta_uri.startswith("postgres://"):
        async_meta_uri = meta_uri.replace("postgres://", "postgresql+asyncpg://")
    elif meta_uri.startswith("postgresql://"):
        async_meta_uri = meta_uri.replace("postgresql://", "postgresql+asyncpg://")
    else:
        raise ValueError("Unsupported meta_db connection type for async SQLAlchemy")

    ms_graph_hook = KiotaRequestAdapterHook(conn_id="ms_graph_api")
    ms_graph_adapter = ms_graph_hook.get_conn()

    delta_hook = BaseHook.get_connection('delta_prod')

    async def _process_all() -> None:
        """Process all persons asynchronously."""

        delta_token_provider = AsyncOAuth2TokenProvider(
            token_url=delta_hook.extra_dejson.get('token_url'),
            client_id=delta_hook.login,
            client_secret=delta_hook.password,
            refresh_margin=30
        )

        async_engine = create_async_engine(
            async_meta_uri,
            echo=False,
            pool_size=10,
            max_overflow=20,
        )

        AsyncSessionLocal = sessionmaker(
            bind=async_engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

        ms_graph_client = GraphServiceClient(request_adapter=ms_graph_adapter)

        async with httpx.AsyncClient(timeout=httpx.Timeout(10.0, read=30.0)) as delta_client:
            with Session(bind=meta_engine) as meta_session:
                persons_with_email = meta_session.query(PersonMedDB).filter(PersonMedDB.email.isnot(None)).all()

                sem = asyncio.BoundedSemaphore(CONCURRENCY)

                async def resolve_user(email: str) -> dict | None:
                    async with sem:
                        user = await delta_get_by_email(client=delta_client, token_provider=delta_token_provider, base_url=delta_hook.host, email=email)
                        if user:
                            return user

                        async with AsyncSessionLocal() as ad_session:
                            user = await skole_ad_get_by_email(session=ad_session, email=email)
                            if user:
                                return user

                        user = await ms_graph_get_user_by_email_alias(client=ms_graph_client, email_alias=email)
                        if user:
                            return user

                        return None

                tasks = [resolve_user(p.email) for p in persons_with_email]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                for person, result in zip(persons_with_email, results):
                    if isinstance(result, Exception):
                        logger.error(f"Lookup error for {person.email}: {result}")
                        continue

                    user = result
                    if not user:
                        continue

                    person.email = user.get("email") or person.email
                    person.name = user.get("name") or person.name
                    person.organization = user.get("unit") or person.organization
                    person.username = user.get("username") or person.username
                    person.found_in_system = True

                meta_session.commit()

        await async_engine.dispose()

    asyncio.run(_process_all())
