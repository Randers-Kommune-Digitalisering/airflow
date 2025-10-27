import requests
import json
from airflow.hooks.base import BaseHook


def send_teams_alert(context):
    conn = BaseHook.get_connection("teams_channel_alarmer")
    webhook_url = conn.host

    dag_id = context.get('dag').dag_id
    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date')
    log_url = context.get('task_instance').log_url

    message = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "summary": f"Airflow Alert: {dag_id}.{task_id}",
        "themeColor": "FF0000",
        "title": f"❌ Task Failed: {dag_id}.{task_id}",
        "sections": [{
            "facts": [
                {"name": "DAG", "value": dag_id},
                {"name": "Task", "value": task_id},
                {"name": "Execution Time", "value": str(execution_date)},
                {"name": "Log URL", "value": log_url}
            ],
            "markdown": True
        }]
    }

    headers = {'Content-Type': 'application/json'}
    requests.post(webhook_url, data=json.dumps(message), headers=headers)
