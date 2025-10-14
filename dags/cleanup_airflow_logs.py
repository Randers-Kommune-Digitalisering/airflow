from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import shutil

LOG_DIR = "/opt/airflow/logs"
THRESHOLD_PERCENT = 90

def cleanup_logs_by_disk_usage():
    total, used, free = shutil.disk_usage(LOG_DIR)
    used_percent = (used / total) * 100

    if used_percent < THRESHOLD_PERCENT:
        print(f"Disk usage is {used_percent:.2f}%, below threshold. No cleanup needed.")
        return

    print(f"Disk usage is {used_percent:.2f}%, starting cleanup...")
    
    log_files = []
    for root, dirs, files in os.walk(LOG_DIR):
        for file in files:
            file_path = os.path.join(root, file)
            try:
                mtime = os.path.getmtime(file_path)
                log_files.append((file_path, mtime))
            except Exception as e:
                print(f"Error accessing {file_path}: {e}")

    log_files.sort(key=lambda x: x[1])

    for file_path, _ in log_files:
        try:
            os.remove(file_path)
            print(f"Deleted: {file_path}")
        except Exception as e:
            print(f"Failed to delete {file_path}: {e}")

        _, used, _ = shutil.disk_usage(LOG_DIR)
        used_percent = (used / total) * 100
        if used_percent < THRESHOLD_PERCENT:
            print(f"Cleanup complete. Disk usage is now {used_percent:.2f}%.")
            break

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='cleanup_logs_by_disk_usage',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Deletes oldest Airflow logs until disk usage is below threshold',
    tags=["maintenance"]
) as dag:

    cleanup_task = PythonOperator(
        task_id='cleanup_logs',
        python_callable=cleanup_logs_by_disk_usage
    )
