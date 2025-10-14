import os
from datetime import datetime
from kubernetes.client import models as k8s
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

NAMESPACE = os.getenv("AIRFLOW_K8S_NAMESPACE")
if not NAMESPACE:
    raise ValueError("Environment variable AIRFLOW_K8S_NAMESPACE is not set. This DAG requires it to determine the Kubernetes namespace.")

with DAG(
    dag_id='log_cleanup_k8s',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['maintenance'],
) as dag:

    cleanup = KubernetesPodOperator(
        task_id='cleanup_airflow_logs',
        name='cleanup-airflow-logs',
        namespace=NAMESPACE,
        image='python:3.12-slim',
        cmds=['python', '-c'],
        arguments=[
            """
import os, shutil, logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("log_cleanup")

LOG_DIR = "/opt/airflow/logs"
MAX_USAGE = 80

def get_usage(path):
    total, used, _ = shutil.disk_usage(path)
    return used / total * 100

def cleanup():
    usage = get_usage(LOG_DIR)
    logger.info(f"Current disk usage: {usage:.2f}%")
    if usage < MAX_USAGE:
        logger.info("Disk usage is below threshold. No cleanup needed.")
        return

    logger.warning("Disk usage above threshold. Starting cleanup...")
    files = sorted(Path(LOG_DIR).rglob("*"), key=lambda f: f.stat().st_mtime)
    for f in files:
        if f.is_file():
            try:
                f.unlink()
                logger.info(f"Deleted: {f}")
                if get_usage(LOG_DIR) < MAX_USAGE:
                    logger.info("Cleanup complete. Disk usage below threshold.")
                    break
            except Exception as e:
                logger.error(f"Error deleting {f}: {e}")

cleanup()
            """
        ],
        volume_mounts=[
            k8s.V1VolumeMount(
                name='logs-pvc',
                mount_path='/opt/airflow/logs'
            )
        ],
        volumes=[
            k8s.V1Volume(
                name='logs-pvc',
                persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                    claim_name='airflow-logs'
                )
            )
        ],
        is_delete_operator_pod=True,
    )
