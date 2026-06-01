# Jobindsats Airflow DAG `README.md`
[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) | [**Konfiguration**](#konfuguration) | [**Afhængigheder**](#afhængigheder) | [**Schedule**](#schedule)

## Formål

Formålet med jobbet er at hente data fra Jobindsats API v3 og gemme det i en PostgreSQL-database.

## Beskrivelse

Koden består af et DAG-job, der udfører følgende trin:

- Henter metadata om tabeller fra Jobindsats API'et (`fetch_and_store_table_updates`)
  - Kalder endpoint: v3/tables?format=json
  - Gemmer resultatet i Postgres-tabellen: jobindsats_table_updates

- For hver konfiguration i Airflow Variable `jobindsats_config`:
  - Henter gyldige perioder for datasættet (perioder hentes fra tabel-metadata)
    - Kalder endpoint: v3/table/{dataset}?format=json
    - Finder perioder for den ønskede periodetype (period_format), fx M, Q, QMAT
  
  - Filtrerer perioder baseret på years_back (beholder perioder fra og med nuværende år minus years_back)
  - Henter data fra Jobindsats API v3 og gemmer i Postgres
    - Kalder endpoint: v3/data/{dataset} med query parameters (format=json, period.<period_format>=... samt øvrige params)
    - Gemmer som tabelnavn: jobindsats_<dataset><id>
      - id er optional og tilføjes som suffix (lowercase), hvis den er angivet i konfigurationen
  

**Dataflow:**
- Data fra Jobindsats API(v3) → Data gemmes i Postgres DB

## Konfuguration

Jobbet læser sin konfiguration fra Airflow Variable jobindsats_config (JSON).

Forventet struktur pr. job:

- name: Navn (bruges bl.a. til kolonnen Periode <name>)
- years_back: Antal år tilbage der hentes perioder for
- dataset: Datasæt-/tabel-id i Jobindsats
- period_format: Periodetype, fx M, Q, QMAT
- params: Ekstra query-parametre til v3/data-kaldet
  - Værdier kan være string eller liste af strings
  - Lister samles til kommaseparerede værdier i query string
- id (valgfri): Tilføjes til output-tabellens navn for at adskille varianter af samme dataset

Eksempel config:

```json
{
  "jobindsats_config": [
    {
      "name": "Offentligt forsørgede",
      "years_back": 2,
      "dataset": "ptv_a02",
      "period_format": "M",
      "params": {
        "mgroup.*": "*",
        "hierarchy._nykom": "*",
        "hierarchy._ygrpa02": ["/5/", "/11/", "/13/", "/14/", "/15/"]
    }
  }
  ]
}
```

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
  - Connection id, Host, port og Password (API key)

- Autentifikation:
  - API key sendes som Bearer token i Authorization-headeren (Authorization: Bearer <password>)

- API Dok ([Jobindats V3 API dokumentation](https://www.jobindsats.dk/information/om-api/brugervejledning-til-version-3/))

## Schedule

Jobbet er sat op til at køre automatisk på følgende tidspunkter:

- **Tidspunkt:** Kl. 00:00 hver mandag
- **Cron syntax:**  
  ```
  0 0 * * 1
  ```