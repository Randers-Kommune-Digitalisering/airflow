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