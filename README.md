# Airflow
[Airflow](https://airflow.apache.org/docs/) er bruges til at planlægge, orkestrere og overvåge workflows.

Workflows (DAGs) er defineret i dags mappen, som også indeholder kode delt imellem workflows. Afhængigheder (Python-biblioteker) kan tilføjes i [requirements.txt](requirements.txt).

Konfiguration for integration med keycloak findes [her](config/webserver/webserver_config.py)

## Kør lokalt
- Installer Docker
- Kør `docker-compose up` i rod-mappen
- Åben localhost:8080 og log ind med brugernavn "airflow" og kodeord "airflow"

## Useful docs
- [DAGs](https://airflow.apache.org/docs/apache-airflow/2.10.5/core-concepts/dags.html)
- [Connections](https://airflow.apache.org/docs/apache-airflow/2.10.5/howto/connection.html)

## Dockerfiles / packages and requirements
Packages needed in DAGs / tasks needs to be added to requirements.txt and will be installed in Dockerfile. If a package is also needed for webserver or scheduler then the requirements.txt and Dockerfile in config/webserver

## Imports
To avoid big imports imports should be done in the task function, like:
```Python
from airflow import DAG
from airflow.operators.python import PythonOperator

def task_my_task(my_var: str):
    from dag_my_dag.my_task import my_task
    return my_task(my_var)

with DAG(
    ...  # DAG Stuff
) as dag:

    run_zylinc = PythonOperator(
        task_id="my_task",
        python_callable=task_my_task
    )
```