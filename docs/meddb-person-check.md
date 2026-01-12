# MEDDB person check Airflow DAG `README.md`
[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afhængigheder) | [**Schedule**](#schedule)

## Formål

Formålet med jobbet er at slå alle emails i MED-databasen op og sætte om de kan findes.

## Beskrivelse

Koden består af et DAG-job, der udfører følgende trin:

- Henter alle personer fra MED-databasen som har en email sat.
- Starter flere sideløbende jobs gør følgende:
  - Slår email op i Delta, hvis den ikke findes gåes til næste skridt
  - Slår email op i Skole AD'et, hvis den ikke findes gåes til næste skridt
  - Slår email op i MS Graph (på e-mail alisas)
  - Hvis ét af opslagene giver resultat opdateres navn, afdeling, brugernavn og at de er fundet i systemet, i MED-databasen.

**Dataflow:**
- Data fra Delta / MS Graph / Skole AD (Meta Postgres DB, schema skolead) → Data gemmes i Meta Postgres DB (schema meddb)

## Afhængigheder

:key: | **Airflow Connections**

**Postgres DB:**
- **`meta_db`**  
  Bruges som `Connection id` i Airflow til at hente host, database, bruger, adgangskode og port til Postgres DB'en meta

**MS Graph API:**
- **`ms_graph_api`**  
  Bruges som `Connection id` i Airflow til at lave en GraphServiceClient

**MS Graph API:**
- **`delta_prod`**  
  Bruges som `Connection id` i Airflow til at hente client_id, client_secret og token_url til Delta produktion.

## Schedule

Jobbet er sat op til at køre en gang om ugen.