# xFlow Nexus Hjælpemidler Airflow DAG `README.md`
[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afh%C3%A6ngigheder) | [**Schedule**](#schedule)

## Formål

Læser xFlow-data fra databasen og indlæser dem i Nexus.

## Beskrivelse

En del af integrationen gennem xFlow og Nexus for hjælpemidler. xFlow sender udfyldte skemaer til et API (med en API-dataaflevering), som indlæser dem i databasen. Denne DAG læser derefter dataene og indsætter dem i Nexus.

**Dataflow:**
- Henter ubehandlede rækker (baseret på statuskolonnen) fra databasen for tabellerne i variablen.
- Henter data fra xFlow for hver række, hvis der er et link til vedhæftninger.
- Indsætter data i Nexus gennem API'et.


**Bemærk:**
- Frontendet, hvor skemaerne indsendes, findes [her](https://www.randers.dk/borger/socialt/hjaelpemidler-og-hjaelp/hjaelpemidler/hjaelpemidler-du-kan-soege/)
- xFlow-dataafleveringen findes [her](https://randers.ditmerflex.dk/randers/Admin/DatabehandlerApi/Edit/69f4e5a8-6f7d-464e-9373-0193e7d9d056)
- De data, som DAG'en behandler, kommer fra en database, der modtager data fra xFlow gennem [external-api](https://github.com/Randers-Kommune-Digitalisering/external-api).

## Afhængigheder

:key: | **Airflow Connections**

**Postgres DB:**
- **Conn Id: `meta_db`**
- **Bitwarden navn: `Meta postgres database prod`**
- **Conn Type: `Postgres`**

Bruges som `Connection ID` i Airflow til at hente host, database, bruger, adgangskode og port til Postgres-databasen. Skemaet `xflow_nexus` er fastkodet i filen `models.py`.

*Påkrævede felter*:
  - Connection ID, Host, Database, Login, Password og Port (5432)

**xFlow API:**
- **Conn Id: `xflow`**
- **Bitwarden navn: `xflow api key xFlow->Nexus`**
- **Conn Type: `HTTP`**

Bruges som `Connection ID` i Airflow til at hente API-nøglen til xFlow-API'et.

*Påkrævet felt*:
  - Password

**Nexus:**
- **Conn Id: `nexus_prod`**
- **Bitwarden navn: `Nexus Randers Drift (client credentials)`**
- **Conn Type**: HTTP

Bruges som `Connection ID` i Airflow til at hente token-URL, base-URL fra host, client ID (Login) og client secret (Password).

*Påkrævede felter*:
  - Connection ID, Host, Login, Password samt ekstraoplysninger med token-URL og logout-URL

### Airflow Variables
:key: | **Airflow Variables**

**Tabeller der skal indlæses i Nexus:**
- **Key**: `xflow_nexus_hjaelpemidler_tables`
En liste over tabeller, der skal indlæses i Nexus.

Eksempel:
```json
["tabel1", "tabel2"]
```

## Schedule

Tidsplanen er sat op til at køre automatisk med følgende interval:

- **Tidspunkt:** hver time
- **Cron syntax:** `@hourly`