# xFlow Nexyus Hjælpemidler Airflow DAG `README.md`
[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afh%C3%A6ngigheder) | [**Schedule**](#schedule)

## Formål

Læser xFlow data fra database og indlæser det i Nexus.

## Beskrivelse

Del af integration igennem xFlow og Nexus for hjælpemidler. XFlow sender udfyldte skemaer til et API (med en API dataaflevering) som indlæser det i database, denne DAG læser så dataet og indsætter det i Nexus.

**Dataflow:**
- Henter udbehandlede rækker (baset på status kolonne) fra databsaen for tabeller i variablen.
- For hver række hentes data fra xFlow (hvis der er link til vedhæftelser)
- Data indsættes i Nexus igennem API'et


**Bemærk:**
- Frontend hvor skemaer indsendes er [her](https://www.randers.dk/borger/socialt/hjaelpemidler-og-hjaelp/hjaelpemidler/hjaelpemidler-du-kan-soege/)
- xFlow dataaflevering er [her](https://randers.ditmerflex.dk/randers/Admin/DatabehandlerApi/Edit/69f4e5a8-6f7d-464e-9373-0193e7d9d056)
- Data DAGen behandler kommer fra en database der får data fra xFlow igennem [external-api](https://github.com/Randers-Kommune-Digitalisering/external-api)

## Afhængigheder

:key: | **Airflow Connections**

**Postgres DB:**
- **Conn Id: `meta_db`**
- **Bitwarden navn: `Meta postgres database prod`**
- **Conn Type: `Postgres`**

 Bruges som `Connection id` i Airflow til at hente host, database, user, pass og port til Postgres DB'en. Schema `xflow_nexus` er hardcoded i models.py filen.

 *Required felter*:
  - Connection id, Host, Database, Login, Password and Port(5432)

**xFlow API :**
- **Conn Id: `xflow`**
- **Bitwarden navn: `xflow api key xFlow->Nexus`**
- **Conn Type: `HTTP`**

Bruges som Connection id i Airflow til at hente api_key til xFlow API'et

*Required felter*:
  - password

**Nexus:**
- **Conn Id: `nexus_prod`**
- **Bitwarden navn: `Nexus Randers Drift (client credentials)`**
- **Conn Type**: HTTP

Bruges som Connection id i Airflow til at hente token url, base url fra host, client id (Login) og client secret (Password).

*Required felter*:
  - Connection id, Host, Login, Password, extra med token url og logout url

### Airflow Variables 
:key: | **Airflow Variables**

**Tabeller der skal indlæses i Nexus:**
- **Key**: `xflow_nexus_hjaelpemidler_tables`
En liste af tabeller der skal indlæses i Nexus

Eksempel:
```json
["tabel1", "tabel2"]
```

## Schedule

Schedule er sat op til at køre automatisk på følgende tidspunkter:

- **Tidspunkt:** hver time
- **Cron syntax:** `@hourly`