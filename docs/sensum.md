# Sensum Airflow DAG `README.md`
[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afh%C3%A6ngigheder) | [**Schedule**](#schedule)

## Formål

Formålet med jobbet er at hente Sensum data fra en SFTP og gemme det i en PostgreSQL-database.

## Beskrivelse

Koden består af et DAG-job, der udfører følgende trin:

- Henter relevante Sensum-filer fra SFTP (`get_files`)
- Læser og behandler filerne, merger data udfra konfigurering
- Dataen gemmes i en Postgres Database

**Dataflow:**
- Data fra SFTP → Data behandles og merges → Data gemmes i Postgres DB

## Afhængigheder

:key: | **Airflow Connections**

**Postgres DB:**
- **`sensum_db`** Bruges som `Connection id` i Airflow til at hente host, database, user, pass og port til Sensum Postgres DB'en

**SFTP:**
- **`sensum_sftp`** Bruges som `Connection id` i Airflow til at hente host, user, pass og port til Sensum SFTP'en

## Schedule

Schedule er sat op til at køre automatisk på følgende tidspunkter:

- **Tidspunkt:** Kl. 00:00 hver søndag
