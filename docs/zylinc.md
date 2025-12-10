# Zylinc Airflow DAG `README.md`
[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afh%C3%A6ngigheder) | [**Schedule**](#schedule)

## Formål

Formålet med jobbet er at hente telefoni data fra flere Zylinc-køer i Elasticsearch og gemme dem i en PostgreSQL-database. Hver kø får sin egen tabel i Postgres databasen.

## Beskrivelse

Kode består af et DAG-job, der udfører følgende trin:

- Connecter til Elasticsearch og henter telefoni data for hver kø i listen
(`get_queue_names()`)
- Følgende data hentes i Elasticsearch: (`QueueName`, `Result`, `AgentDisplayName`, `ConversationEventType`, `StartTimeUtc`, `TotalDurationInMilliseconds`, `EventDurationInMilliseconds`)
- Dataen gemmes i en Postgres Database, én tabel pr. kø (`zylinc_<kønavn>`)

**Dataflow:**
- Data fra Elasticsearch → Hver kø gemmes i deres egen tabel i Postgres DB

## Afhængigheder

:key: | **Airflow Connections**

**Postgres DB:**
- **`zylinc_db`** Bruges som Connection id i Airflow til at hente host, database, user, pass og port til Postgres DB'en

**Elasticsearch**
- **`zylinc_elasticsearch`** Bruges som Connection id i Airflow til at hente host, schema, user, pass og port til Elasticsearch


## Schedule

Schedule er sat op til at køre automatisk på følgende tidspunkter:

- **Tidspunkt:** Kl. 00:00 hver eneste døgn  
- **Cron syntax:**  
  ```
  0 0 * * *
  ```