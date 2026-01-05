import asyncio
import logging
import httpx

from sqlalchemy.orm import Session

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from dag_meddb_person_check.model import PersonMedDB
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.azure.hooks.msgraph import KiotaRequestAdapterHook
from msgraph.graph_service_client import GraphServiceClient

from dag_meddb_person_check.lookups import delta_get_by_email, ms_graph_get_user_by_email_alias, skole_ad_get_by_email


logger = logging.getLogger(__name__)
CONCURRENCY = 10


# Helper function
async def _get_bearer_token_async(token_url: str, client_id: str, client_secret: str, scope: str | None = None,) -> str:
    """
    Fetch an OAuth2 access token via client credentials (async).
    """
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    if scope:
        data["scope"] = scope

    async with httpx.AsyncClient(timeout=httpx.Timeout(10.0, read=20.0)) as client:
        resp = await client.post(
            token_url,
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        resp.raise_for_status()
        token = resp.json().get("access_token")
        if not token:
            raise RuntimeError("No access_token in token response")
        return token


def _make_delta_async_client(base_url: str, bearer_token: str) -> httpx.AsyncClient:
    """
    Create an AsyncClient that includes the bearer token in all requests.
    """
    return httpx.AsyncClient(
        base_url=base_url,
        headers={"Authorization": f"Bearer {bearer_token}"},
        timeout=httpx.Timeout(10.0, read=30.0),
    )


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

        token = await _get_bearer_token_async(
            token_url=delta_hook.extra_dejson.get('token_url'),
            client_id=delta_hook.login,
            client_secret=delta_hook.password
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

        async with _make_delta_async_client(delta_hook.host, token) as delta_client:
            with Session(bind=meta_engine) as meta_session:
                persons = meta_session.query(PersonMedDB).all()
                persons_with_email = [p for p in persons if p.email]

                sem = asyncio.BoundedSemaphore(CONCURRENCY)

                async def resolve_user(email: str) -> dict | None:
                    async with sem:
                        user = await delta_get_by_email(delta_client, delta_hook.host, email)
                        if user:
                            return user

                        async with AsyncSessionLocal() as ad_session:
                            user = await skole_ad_get_by_email(ad_session, email)
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
