from datetime import timedelta


DEFAULT_DAG_ARGS = {
    'owner': 'all',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    "email": ["digitalisering@randers.dk"],
    "email_on_failure": True,
    "email_on_retry": False,
}

VOGNPARK_SFTP_DIR = '/Vognpark/'
JOBINDSATS_HTTP_CONN_ID = "jobindsats_api"
JOBINDSATS_DB_CONN_ID = "jobindsats_dbB"
