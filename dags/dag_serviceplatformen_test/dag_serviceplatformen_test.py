from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

from pendulum import datetime, timezone
from utils.config import DEFAULT_DAG_ARGS

dag_args = DEFAULT_DAG_ARGS.copy()
dag_args["retries"] = 0

with DAG(
    dag_id="serviceplatformen_test_bash",
    start_date=datetime(year=2026, month=4, day=28, tz=timezone("Europe/Copenhagen")),
    schedule=None,
    catchup=False,
    default_args=dag_args,
    description="Testing serviceplatformen integration",
    tags=["serviceplatformen", "test", "kombit", "bash"],
) as dag:
    DAG_DIR = Path(__file__).parent
    testing_py_path = DAG_DIR / "testing.py"

    run_bash = BashOperator(
        task_id="run_bash",
        bash_command=f"python {testing_py_path}",
    )

with DAG(
    dag_id="serviceplatformen_test_python",
    start_date=datetime(year=2026, month=4, day=28, tz=timezone("Europe/Copenhagen")),
    schedule=None,
    catchup=False,
    default_args=dag_args,
    description="Testing serviceplatformen integration",
    tags=["serviceplatformen", "test", "kombit", "python"],
) as dag:
    from dag_serviceplatformen_test.testing import test

    run_python = PythonOperator(
        task_id="run_python",
        python_callable=test,
    )

with DAG(
    dag_id="serviceplatformen_test_kubernetes",
    start_date=datetime(year=2026, month=4, day=28, tz=timezone("Europe/Copenhagen")),
    schedule=None,
    catchup=False,
    default_args=dag_args,
    description="Testing serviceplatformen integration",
    tags=["serviceplatformen", "test", "kombit", "kubernetes"],
) as dag:

    run_kubernetes = KubernetesPodOperator(
        task_id="run_kubernetes",
        name="run-testing-py",
        image="ghcr.io/randers-kommune-digitalisering/airflow-serviceplatformen:prod",
        cmds=["python"],
        arguments=["testing.py"],
        get_logs=True,
        env=[
            {
                "name": "CLIENT_CERT_PRIVATE_KEY",
                "valueFrom": {
                    "secretKeyRef": {
                        "name": "cpr-replika-auth",
                        "key": "client_cert_privat"
                    }
                }
            },
            {
                "name": "CLIENT_CERT_PUBLIC_KEY",
                "valueFrom": {
                    "secretKeyRef": {
                        "name": "cpr-replika-auth",
                        "key": "client_cert_public"
                    }
                }
            }
        ]
    )
