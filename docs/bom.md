# BOM Airflow DAG
[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afh%C3%A6ngigheder) | [**Schedule**](#schedule)

## Formål

Formålet med jobbet er at hente Nøgletal for byggesager fra Byg og Miljø (BOM) og gemme resultatet i en Postgres DB. Data hentes for både den seneste afsluttede måned og et glidende gennemsnit for de seneste 12 måneder.

## Beskrivelse

Koden består af et DAG-job, der udfører følgende trin:

1. Logger ind på BOM med brugernavn og adgangskode fra Airflow Connection `bom_login`.
2. Navigerer til **Statistik og Servicemål**.
3. Vælger sagsområdet `Byg` og de relevante servicemål.
4. Henter Nøgletal for perioden fra første dag i forrige måned til første dag i den aktuelle måned.
5. Henter `Glidende gennemsnit` Nøgletal for perioden fra første dag 12 måneder tilbage til første dag i den aktuelle måned.
6. Udtrækker følgende felter fra Nøgletal-tabellen:
	 - `Fra Dato`
	 - `Til Dato`
	 - `Kategori`
	 - `Sagsbehandlingstid`
	 - `Servicemål i procent`
7. Gemmer de månedlige data i tabellen `bom_data_monthly`.
8. Gemmer data for det glidende gennemsnit i tabellen `bom_data_glidende`.

**Dataflow:**
- BOM -> Playwright-udtræk af Nøgletal -> Postgres DB.

## Afhængigheder

:key: | **Airflow Connections**

**BOM login:**
- **`bom_login`**
- **Bitwarden navn: `Robotbruger til BOM`**

**Conn Type**: HTTP

Bruges til at hente brugernavn og adgangskode til BOM/ADFS-login.

*Required felter*:
- Connection id, Login og Password

**Postgres database:**
- **`byggesager`**

**Conn Type**: Postgres

Bruges til at gemme de udtrukne Nøgletal i tabellerne `bom_data_monthly` og `bom_data_glidende`.

*Required felter*:
- Connection id, Host, Database, Login, Password and Port(5432)

## Schedule

Jobbet er sat op til at køre automatisk på følgende tidspunkt:

- **Schedule:** `0 0 1 * *` (kl. 00:00 den første dag i hver måned)
