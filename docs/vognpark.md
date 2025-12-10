# Vognpark Airflow DAG `README.md`
[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afh%C3%A6ngigheder) | [**Schedule**](#schedule)

## Formål

Formålet med jobbet er at hente vognpark data fra en SFTP og gemme det i en PostgreSQL-database.

## Beskrivelse

Kode består af et DAG-job, der udfører følgende trin:

- Henter den seneste fil fra SFTP'en
(`get_latest_vognpark_excel_path`) og læser Excel filen (`read_vognpark_excel_from_sftp`)
- Dataen gemmes i en Postgres Database

**Dataflow:**
- Data fra SFTP → Data gemmes i Postgres DB

## Afhængigheder

:key: | **Airflow Connections**

**Postgrs DB:**
- **`vognpark_db`** Bruges som `Connection id` i Airflow til at hente host, database, user, pass og port til Postgres DB'en

**SFTP:**
- **`shared_sftp`** Bruges som `Connection id` i Airflow til at hente host, schema, user, pass og port til SFTP'en


## Schedule

Schedule er sat op til at køre automatisk på følgende tidspunkter:

- **Tidspunkt:** Kl. 00:00 hver mandag
- **Cron syntax:**  
  ```
  0 0 * * 1
  ```