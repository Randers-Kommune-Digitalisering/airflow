# Gis to Dalux Airflow DAG `README.md`
[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afhængigheder) | [**Schedule**](#schedule)

## Formål

Formålet med jobbet er at hente opdateret bygnings data fra GIS databasen og opdaterer det i Dalux FM gennem API

## Beskrivelse

Koden består af et DAG-job, der udfører følgende trin:

- Henter bygningsdata fra GIS-databasen via SQL-kald
- Henter eksisterende bygninger fra Dalux FM og bygger et lookup-indeks på `(estate_id, bbr_bygningsnummer)` for at kunne matche/deduplicere
- For hver bygning fra GIS:
  - Finder “effective” Dalux buildingId:
    - Hvis `id/building_id` findes i GIS, bruges den direkte
    - Ellers forsøges match i Dalux via `(estate_id, bbr_bygningsnummer)`
    - Hvis der ikke findes et match, oprettes bygningen i Dalux FM:
      - `name` sættes til BBR-nummer
      - `alternativeName` sættes fra GIS (fx kaldenavn)
      - `estateRef.estateId` sættes fra `estate_id`
      - Hvis muligt parses `ejendom_adresse` og sendes som Dalux `address` (road/number/zipCode/city)
  - Opdaterer relevante felter i Dalux FM via Dalux API’et (PATCH med kun de felter der skal ændres):
    - Standardfelter: fx `name` (BBR-nummer) og `alternativeName`
    - De gamle Brugerdefinerede felter kan ikke bruges med API'et da "Historik" er slået til. Når først Historik er slået til så kan man ikke fjerne det igen([Brugerdefinerede felter](https://support.dalux.com/hc/da/articles/11723141799836-Brugerdefinerede-felter#h_01HHHFKNF06BRTBA3KPMVJJ83G:~:text=N%C3%A5r%20der%20oprettes,ikke%20%C3%A6ndres%20bagefter.)). Derfor er der blevet lavet helt nye Brugerdefinerede felter i Dalux FM.
    - Brugerdefinerede felter (userDefinedFields) med de nye feltnavne(Uden histoik enabled i Dalux FM):
      - `Ny SBSYS`
      - `Ny Institutionsnavn` (trimmes for whitespace)
      - `Ny Servicedistrikt` (normaliseres, fx fjern “Distrikt”)
      - `Ny KL Anvendelsesområde`
      - `Ny KL Ejerskab`
      - `Ny KL BDA areal` (sendes som integer når muligt)
  - Hvis bygningen har geometri:
    - Konverteres geometri fra UTM32/WKT-format til WGS84
    - Opdateres som polygon i Dalux FM

**Dataflow:**
- Data fra GIS-databasen → match/ dedupe mod Dalux → (opret hvis mangler) → opdater felter i Dalux → opdater polygon (hvis geometri findes)

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