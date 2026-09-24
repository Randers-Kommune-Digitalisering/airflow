# Frontdesk Airflow DAG `README.md`
[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) |
[**Afhængigheder**](#afh%C3%A6ngigheder) | [**Schedule**](#schedule) 

## Formål

Formålet med jobbet er at hente rå Frontdesk-operationer fra MSSQL, transformere data til rapportering og prognoser og gemme resultatet i Postgres.

## Beskrivelse

**Jobbet ufører følgende trin:**

- Opretter forbindelse til Frontdesk-kildedatabasen
- Henter rå operationer fra Operation-tabellen
- Validerer at nødvendige kolonner findes
- Normaliserer dato- og tidsfelter samt fjerner irrelevante kolonner
- Filtrerer rækker:
    - Ekskluderer tællere, der er uden for Borgerservise-scope
    - beholder kun nyere daya (seneste 2 år)
- Giver data felter
    - Dato, uge og år
    - Queue-gruppering
    - Ventetid og behandlingstid
- Gemmer transformerede operationer i Postgres-tabellen operations
- Beregner forecast med Prophet
    - Samlet forecast
    - Kun hverdage medtages
- Gemmer forecast i Postgres-tabellen forecasts

Hvis der ikke er data efter filtrering, eller forecast-output er ugyldigt, stopper jobbet bevidst med fejl.

**Dataflow:**
- Frontdesk MSSQL -> transformering og forecast -> Postgres (operations og forecasts)

## Afhængigheder
:key: | **Airflow Connections**
**Frontdesk-kilde (MSSQL)**
- **`azure_frontdesk_db`**

**Conn type**: MYSSQL

bruges som `Connection id`i Airflow til at hente host, database, user, pass og port til Frontdesk-kilden

*Required felter*:
- Connection id, Host, DataBase, Login, Password og Port

**Frontdesk-mål (Postgres)**:
- **`frontdesk_db`**

**Conn type**: Postgres

Bruges som `Connection id` i Airflow til at gemme operations- og forecast-data i Postgres

*Required felter*:
- Connection id, Host, Database, User, Pass og Port'

## Schedule

Schedule er sat op til at køre automatisk på følgende tidspunkter:

- **Tidspunkt:** hver uge 
