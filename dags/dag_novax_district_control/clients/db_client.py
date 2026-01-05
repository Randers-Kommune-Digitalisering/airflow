from airflow.providers.postgres.hooks.postgres import PostgresHook
import datetime as dt
from sqlalchemy.orm import Session as SqlalchemySession
from dag_novax_district_control.model import NovaxHistory, NovaxRecord, Base


meta_db_engine = None


def _normalize_to_datetime(value: object, *, as_end: bool = False) -> dt.datetime | None:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value
    if isinstance(value, dt.date):
        return dt.datetime.combine(value, dt.time.max if as_end else dt.time.min)
    return None


def ensure_tables_exist(engine=None) -> None:
    """
    Ensure all tables defined in the model exist in the database.
    """
    if engine is None:
        engine = get_db_engine()
    Base.metadata.create_all(engine)


def get_db_engine() -> object:
    global meta_db_engine
    if meta_db_engine is not None:
        return meta_db_engine
    meta_hook = PostgresHook(postgres_conn_id="meta_db")
    meta_db_engine = meta_hook.get_sqlalchemy_engine()
    return meta_db_engine


def get_db_session() -> SqlalchemySession:
    return SqlalchemySession(bind=get_db_engine())


def get_last_run_info() -> dict:
    """
    Retrieve the last run information from the database.
    Returns a dictionary with keys 'id', 'last_run_start_date', 'last_run_end_date' and 'completed'.
    """
    ensure_tables_exist()
    engine = get_db_engine()
    with SqlalchemySession(bind=engine) as meta_session:
        history = meta_session.query(NovaxHistory).all()
        if history:
            last_run = history[-1]
            return {
                'id': last_run.id,
                'last_run_start_date': last_run.start_date,
                'last_run_end_date': last_run.end_date,
                'completed': last_run.completed
            }
        else:
            return {
                'last_run_start_date': None,
                'last_run_end_date': None,
                'completed': None
            }


def create_novax_run_record(start_date, end_date) -> int:
    """
    Create a new NovaxHistory record in the database.

    :param start_date: The start date of the run.
    :param end_date: The end date of the run.
    A new run is created with completed=False; it can be updated to True when finished.
    """

    # Normalize inputs to datetimes for the SQLAlchemy DateTime columns
    if isinstance(start_date, dt.date) and not isinstance(start_date, dt.datetime):
        start_date = dt.datetime.combine(start_date, dt.time.min)
    if isinstance(end_date, dt.date) and not isinstance(end_date, dt.datetime):
        end_date = dt.datetime.combine(end_date, dt.time.min)

    engine = get_db_engine()
    ensure_tables_exist(engine=engine)
    with SqlalchemySession(bind=engine) as meta_session:
        new_run = NovaxHistory(
            ts=dt.datetime.utcnow(),
            start_date=start_date,
            end_date=end_date,
            completed=False,
        )
        meta_session.add(new_run)
        # Ensure PK is populated before commit/session close to avoid
        # DetachedInstanceError (SQLAlchemy expires instances on commit).
        meta_session.flush()
        run_id = new_run.id
        meta_session.commit()

    return run_id


def update_novax_run_record(run_id: int, completed: bool) -> bool:
    """
    Update an existing NovaxHistory record in the database.

    :param run_id: The ID of the run to update.
    :param completed: The completion status to set.
    """
    engine = get_db_engine()
    ensure_tables_exist(engine=engine)
    with SqlalchemySession(bind=engine) as meta_session:
        run_record = meta_session.query(NovaxHistory).filter(NovaxHistory.id == run_id).first()
        if run_record:
            run_record.completed = completed
            meta_session.commit()
    return True if run_record else False


def create_novax_record(nameid: int, success: bool, runid: int) -> int:
    """
    Create a new NovaxRecord in the database.

    :param nameid: The name ID associated with the record.
    :param success: The success status of the record.
    :param runid: The ID of the associated NovaxHistory run.
    """
    engine = get_db_engine()
    ensure_tables_exist(engine=engine)
    with SqlalchemySession(bind=engine) as meta_session:
        new_record = NovaxRecord(
            nameid=nameid,
            success=success,
            runid=runid,
        )
        meta_session.add(new_record)
        meta_session.flush()
        record_id = new_record.id
        meta_session.commit()

    return record_id


def get_processed_nameids_in_period(
    start_date: dt.date | dt.datetime,
    end_date: dt.date | dt.datetime,
    *,
    completed_runs_only: bool = False,
    successful_records_only: bool = True,
) -> set[str]:
    """Return distinct NovaxRecord.nameid processed in runs overlapping a period.

    Overlap condition:
      run.start_date <= end_date AND run.end_date >= start_date
    """
    start_dt = _normalize_to_datetime(start_date, as_end=False)
    end_dt = _normalize_to_datetime(end_date, as_end=True)
    if start_dt is None or end_dt is None:
        return set()

    engine = get_db_engine()
    ensure_tables_exist(engine=engine)

    with SqlalchemySession(bind=engine) as meta_session:
        query = (
            meta_session.query(NovaxRecord.nameid)
            .join(NovaxHistory, NovaxRecord.runid == NovaxHistory.id)
            .filter(NovaxHistory.start_date <= end_dt)
            .filter(NovaxHistory.end_date >= start_dt)
        )

        if completed_runs_only:
            query = query.filter(NovaxHistory.completed.is_(True))
        if successful_records_only:
            query = query.filter(NovaxRecord.success.is_(True))

        rows = query.distinct().all()
        return {str(row[0]) for row in rows if row and row[0] is not None}
