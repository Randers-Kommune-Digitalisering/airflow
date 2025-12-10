from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy.orm import Session as SqlalchemySession


def get_db_engine():
    meta_hook = PostgresHook(postgres_conn_id="meta_db")
    meta_engine = meta_hook.get_sqlalchemy_engine()
    return meta_engine


def get_db_session() -> SqlalchemySession:
    return SqlalchemySession(bind=get_db_engine())
