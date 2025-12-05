from datetime import timedelta


DEFAULT_DAG_ARGS = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    "email": ["digitalisering@randers.dk"],
    "email_on_failure": True,
    "email_on_retry": False,
}
