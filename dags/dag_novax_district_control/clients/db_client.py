from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy.orm import Session as SqlalchemySession
from dag_novax_district_control.model import NovaxHistory, Base


def ensure_tables_exist():
    """
    Ensure all tables defined in the model exist in the database.
    """
    engine = get_db_engine()
    Base.metadata.create_all(engine)


def get_db_engine():
    meta_hook = PostgresHook(postgres_conn_id="meta_db")
    meta_engine = meta_hook.get_sqlalchemy_engine()
    return meta_engine


def get_db_session() -> SqlalchemySession:
    return SqlalchemySession(bind=get_db_engine())


def get_last_run_info() -> dict:
    """
    Retrieve the last run information from the database.
    Returns a dictionary with keys 'last_run_start_date', 'last_run_end_date' and 'status'.
    """
    ensure_tables_exist()
    engine = get_db_engine()
    with SqlalchemySession(bind=engine) as meta_session:
        history = meta_session.query(NovaxHistory).all()
        if history:
            last_run = history[-1]
            return {
                'last_run_start_date': last_run.start_date,
                'last_run_end_date': last_run.end_date,
                'status': last_run.status
            }
        else:
            return {
                'last_run_start_date': None,
                'last_run_end_date': None,
                'status': None
            }

    # with engine.connect() as conn:
    #
    #     result = conn.execute("SELECT last_run_date, status FROM novax_district_control_runs ORDER BY last_run_date DESC LIMIT 1")
    #     row = result.fetchone()
    #     if row:
    #         return {
    #             'last_run_date': row['last_run_date'],
    #             'status': row['status']
    #         }
    #     else:
    #         return {
    #             'last_run_date': None,
    #             'status': None
    #         }
