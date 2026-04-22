# Ekko User Sync `README.md`
[**Formål**](#formål) | [**Dataflow**](#dataflow) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afhængigheder) | [**Schedule**](#schedule)

## Formål
Upload fil til Ekko App server med relevante bruger- / ansættelses-data (synkronosering af brugere).

## Dataflow
Læser ansættelsesdata og persondata for ansatte i Ejendomsservie (baseret på liste af SD afdelinger) fra SD -> genererer en csv fil med de relevante data -> uploader filen til Ekko apps FTPS server.

## Afhængigheder

:key: | **Airflow Connections**

**Delta API:**
- **`sd_silkeborgdata`**

**Conn Type**: HTTP

Auth til SD API

*Påkrævede felter*:
  - Connection id, Host, Login, Password.

**Nexus:**
- **`ekko_ftps`**
- **Bitwarden navn: `Nexus Randers Drift (client credentials)`**

**Conn Type**: FTP

*Required felter*:
  - Connection id, Host, Login, Password, Port

### Airflow Variables
**Ekko SD afdelinger**
- **Key:** `ekko_sd_departments`
- **Format (JSON):** `[<id>, <id>, ...]`
- **Beskrivelse*:** Liste af id'er for SD afdelinger

## Schedule
Schedule er sat op til at køre automatisk på følgende tidspunkter:

- **Tidspunkt:** @daily (Hver dag ved midnat)
