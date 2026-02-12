# Asset Airflow DAG `README.md`
[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afh%C3%A6ngigheder) | [**Schedule**](#schedule)

## Formål

Formålet med jobbet er at samle alt Asset data fra Randers Kommune i et samlet sted. Data hentes fra flere forskellige kilder: `Capa CMS DB, Historisk Data fra SFTP, Atea & Delta.` Alt data bliver gemt i en Postgres DB, hvor der til sidst laves en query af DB'en hvor man uploader alt relavant Asset data til Topdesk.

## Beskrivelse

Koden består af et DAG-job, der udfører følgende trin:

- Henter største delen af Asset data fra CAPA CMS DB (`insert_departments_data`, `insert_users_data`, `insert_computers_data`)
- Historisk data fra tidligere leverandør hentes på en SFTP fra Comm2ig og Atea samt hentes Device License (`insert_device_license_and_historical_data`)
- Henter Købspris, Købsdato og Garantiudløb fra Atea API (`insert_atea_data`)
- Henter Afdelings EAN fra Delta API (`insert_department_ean_from_delta`)
- Dataen gemmes i en Postgres Database
- Til sidst bliver relavant Asset data automatisk importeret til TopDesk(`upload_assets_to_topdesk`)

**Dataflow:**
- Data fra forskellige kilder → Data gemmes i Postgres DB → Automatisk import til TopDesk

**Airflow workflow:**

t_create_tables

t_departments

Parallel: t_users og t_delta_ean

Efter t_users: t_computers → t_atea → t_device_license

Når både t_delta_ean og t_device_license er færdig køres: t_upload_topdesk

## Afhængigheder

:key: | **Airflow Connections**

**MSSQL DB:**
- **`capa_cms_db`**

**Conn Type**: Microsoft SQL Server

Bruges som `Connection id` i Airflow til at hente host, schema, login, password og port til CAPA CMS DB'en

*Required felter*:
  - Connection id, Host, Schema, Login, Password and Port(1433)

**Postgres DB:**
- **`asset_db`**

**Conn Type**: Postgres

Bruges som `Connection id` i Airflow til at hente host, database, user, pass og port til Asset Postgres DB'en

*Required felter*:
  - Connection id, Host, Database, Login, Password and Port(5432)

**SFTP:**
- **`asset_sftp`**

**Conn Type**: SFTP

 Bruges som `Connection id` i Airflow til at hente host, user, pass og port til Asset SFTP'en

 *Required felter*:
  - Connection id, Host, Username, Password og Port(22)

**Atea API:**
- **`atea_api`**

  **Conn Type**: HTTP

  Bruges som `Connection id` i Airflow til at hente host og SubKey til Atea API'et

  *Required felter*:
  - Connection id, Host og Password

**Delta Prod API:**
- **`delta_prod`**

  **Conn Type**: HTTP

  Bruges som `Connection id` i Airflow til at hente host, login, password og token under extras til Delta API'et

  *Required felter*:
  - Connection id, Host, Login, Password og Extra(Skal indeholde token_url)
  - f.eks: {
    "token_url": x,
  }

**Topdesk Test(For nu) API:**
- **`topdesk_api_test`**

  **Conn Type**: HTTP

  Bruges som `Connection id` i Airflow til at hente host, login og password til TopDesk Test API'et.

  *Required felter*:
  - Connection id, Host, Login og Password


:key: | **Airflow Variables**

**Fil config path for Asset Data:**

  **Key**: asset_config

  Bruges til at hente hardcoded file paths. F.eks. til SFTP fil paths og TopDesk Filnavn

  *Required felter*:
  - Key og Val
  - f.eks: Val {
  "device_license_file_path": "x.csv",
  "comm2ig_historical_file_path": "x.csv",
  "ean_atea_file_path": "x",
  "topdesk_file_path": "x"
}

## Schedule

Schedule er sat op til at køre automatisk på følgende tidspunkter:

- **Tidspunkt:** Kl. 00:00 hvert døgn
