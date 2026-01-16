# Jobindsats Airflow DAG `README.md`
[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afhængigheder) | [**Schedule**](#schedule)

## Formål

Formålet med jobbet er at hente data fra Jobindsats API'et og gemme det i en PostgreSQL-database

## Beskrivelse

Koden består af et DAG-job, der udfører følgende trin:

- Henter metadata om tabeller fra Jobindsats API'et (`fetch_and_store_table_updates`)
- For hver konfigureret job/datasæt i `JOBINDSATS_CONFIG`:
  - Henter de nyeste tilgængelige data fra Jobindsats API'et (`get_data`)
  - Transformer og indlæser dataen i en Postgres-database

**Dataflow:**
- Data fra Jobindsats API → Data gemmes i Postgres DB

## Afhængigheder

:key: | **Airflow Connections**

**Postgres DB:**
- **`jobindsats_db`**  

  **Conn Type**: Postgres

  Bruges som `Connection id` i Airflow til at hente host, database, bruger, adgangskode og port til Postgres DB'en

  *Required felter*:
  - Connection id, Host, Database, Login, Password and Port(5432)

**Jobindsats API:**
- **`jobindsats_api`**

  **Conn Type**: HTTP

  Bruges som `Connection id` i Airflow til at hente host og API-nøgle til Jobindsats API'et

  *Required felter*:
  - Connection id, Host, port og Extra(Skal indeholde api_key)
  - f.eks: {
    "api_key": x,
  }

## Schedule

Jobbet er sat op til at køre automatisk på følgende tidspunkter:

- **Tidspunkt:** Kl. 00:00 hver mandag
- **Cron syntax:**  
  ```
  0 0 * * 1
  ```