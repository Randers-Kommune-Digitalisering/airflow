# Byggesagsstatistik SBSYS Airflow DAG `README.md`
[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) | [**Konfiguration**](#konfiguration) | [**Afhængigheder**](#afhængigheder) | [**Schedule**](#schedule)

## Formål

Formålet med jobbet er at hente byggesagsstatistik-data fra SBSYS (MSSQL) og gemme det i byggesager-databasen (Postgres).

## Beskrivelse

Koden består af et DAG-job, der udfører følgende trin:

- Læser DAG-parametret `sync_start_date` (default: `2020-01-01`)
  - Validerer at værdien er i gyldigt ISO-format (fx `YYYY-MM-DD`)
  - Ved ugyldigt format fejler jobbet med en valideringsfejl

- Læser Airflow Variablen `byggesagsstatistik_sbsys` (JSON)
  - Kræver felterne `GROUPINGS` og `SKABELON_IDS`
  - Konfigurationen normaliseres til integer-id'er

- Opretter forbindelse til:
  - SBSYS MSSQL (kilde)
  - Byggesager Postgres (mål)

- Synkroniserer metadata-tabeller:
  - `BeslutningsType` -> `Beslutningstype`
  - `ByggeSagKode` -> `Byggesagskode`
  - `SagSkabelon` (filtreret på `SKABELON_IDS`) -> `Byggesagskode`

- Vedligeholder byggesagsgrupper i mål-databasen:
  - Finder eller opretter `Byggesagsgruppe` pr. navn fra `GROUPINGS`
  - Mapper byggesagskoder til korrekt gruppe via konfigurationen

- Synkroniserer byggesagsdata fra og med `sync_start_date`:
  - `ByggeSag` (filtreret på `Modtaget >= sync_start_date`) -> `ByggesagByg`
  - `Sag` (filtreret på `SkabelonID in SKABELON_IDS` og `Created >= sync_start_date`) -> `ByggesagSag`

- Gemmer ændringer med én commit i mål-databasen

**Dataflow:**
- SBSYS MSSQL -> SQLAlchemy/ORM-transform -> Byggesager Postgres

## Konfiguration

### DAG params

- `sync_start_date` (string, default: `2020-01-01`)
  - Beskriver nedre datogrænse for hvilke SBSYS-records der synkroniseres

### Airflow Variables

**Byggesagsstatistik SBSYS konfiguration:**
- **Key:** `byggesagsstatistik_sbsys`

*Required felter*:
- `GROUPINGS` (objekt: gruppenavn -> liste af byggesagskode-id'er)
- `SKABELON_IDS` (liste af skabelon-id'er)

Eksempel:
```json
	{
    "SKABELON_IDS": [5837, 5846, 6378, 6388, 6400, 6403, 6453, 6454, 6455],
    "GROUPINGS": {
        "Industri og lager": [1, 36],
        "Sekundært byggeri": [2, 35, 29, 44, 27, 5846],
        "Erhverv": [3, 8, 40, 9, 12, 13, 14, 28, 37, 32, 38, 5837, 6378, 6388, 6400],
        "Enfamiliehuse": [4, 5, 15, 17, 10, 16, 18, 24, 25, 26],
        "Etageejendomme": [6, 7, 11, 31, 33],
        "Landzone": [41, 42, 43, 6403, 6453, 6454, 6455]
    }
}
```

## Afhængigheder

:key: | **Airflow Connections**

**SBSYS MSSQL (kilde):**
- **`sbsys-byggesager`**

**Conn Type**: Microsoft SQL Server

Bruges til at hente data fra SBSYS-tabeller via SQLAlchemy/ORM.

*Required felter*:
- Connection id, Host, Schema, Login, Password and Port(1433)

**Byggesager Postgres (mål):**
- **`byggesager`**

**Conn Type**: Postgres

Bruges til at skrive/sammenflette data i byggesager-databasen.

*Required felter*:
- Connection id, Host, Database, Login, Password and Port(5432)

## Schedule
Schedule er sat op til at køre automatisk på følgende tidspunkter:

- **Tidspunkt:** `@monthly` (hver måned; den første ved midnat)