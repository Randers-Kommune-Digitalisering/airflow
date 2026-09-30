from datetime import timedelta


DEFAULT_DAG_ARGS = {
    'owner': 'all',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    "email": ["udvikling@randers.dk"],
    "email_on_failure": True,
    "email_on_retry": False,
}
