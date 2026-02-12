# Gis to Dalux Airflow DAG `README.md`
[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afhængigheder) | [**Schedule**](#schedule)

## Formål

Formålet med jobbet er at hente opdateret bygnings data fra GIS databasen og opdaterer det i Dalux FM gennem API

## Beskrivelse

Koden består af et DAG-job, der udfører følgende trin:

- Henter bygningsdata fra GIS-databasen via SQL kald
- For hver bygning opdateres relevante felter i Dalux FM via Dalux API’et:
  - Opdaterer både standardfelter (fx navn, alternativt navn) og brugerdefinerede felter (fx SBSYS, Institutionsnavn, Servicedistrikt, KL Anvendelsesområde, KL Ejerskab).
  - Hvis bygningen har geometri, konverteres denne fra UTM32/WKT-format til WGS84 og opdateres som polygon i Dalux FM.

**Dataflow:**
- Data fra GIS-databasen → Data opdateres i Dalux FM via API’et

## Afhængigheder

:key: | **Airflow Connections**

**Postgres DB:**
- **`gis_db`**

  **Conn Type**: Postgres

  Bruges som `Connection id` i Airflow til at hente host, database, bruger, adgangskode og port til Postgres DB'en

  *Required felter*:
  - Connection id, Host, Database, Login, Password and Port(5432)

**Dalux FM API:**
- **`dalux_api`**

  **Conn Type**: HTTP

  Bruges som `Connection id` i Airflow til at hente host og API-nøgle til Dalux FM API'et

  *Required felter*:
  - Connection id, Host, port og Extra(Skal indeholde api_key)
  - f.eks: {
    "api_key": x,
  }

## Schedule

Jobbet er sat op til at køre automatisk på følgende tidspunkter:

- **Tidspunkt:** Kl. 00:00 hver eneste døgn  
- **Cron syntax:**  
  ```
  0 0 * * *
  ```