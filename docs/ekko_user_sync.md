
# Ekko User Sync `README.md`
[**Formål**](#formål) | [**Dataflow**](#dataflow) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afhængigheder) | [**Schedule**](#schedule)

## Formål
Uploader fil til Ekko App-server med relevante bruger- og ansættelsesdata (synkronisering af brugere).

## Dataflow
Læser ansættelsesdata og persondata for ansatte i Ejendomsservice (baseret på liste af SD-afdelinger) fra SD → genererer en CSV-fil med de relevante data → uploader filen til Ekko apps FTPS-server.


## Afhængigheder

:key: | **Airflow Connections**

**Delta API:**
- **`sd_silkeborgdata`**

**Conn Type**: HTTP

Auth til SD API

*Påkrævede felter*:
  - Connection ID, Host, Login, Password.

**Ekko app-server:**
- **`ekko_ftps`**
- **Bitwarden-navn: `EKKO app FTPS`**

**Conn Type**: FTP

*Påkrævede felter*:
  - Connection ID, Host, Login, Password, Port

### Airflow Variables
**Ekko SD-afdelinger**
- **Key:** `ekko_sd_departments`
- **Format (JSON):** `[<id>, <id>, ...]`
- **Beskrivelse:** Liste af ID'er for SD-afdelinger

## Schedule
Schedule er sat op til at køre automatisk på følgende tidspunkter:

- **Tidspunkt:** @daily (hver dag ved midnat)
