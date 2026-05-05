
# Ekko User Sync `README.md`
[**Formål**](#formål) | [**Dataflow**](#dataflow) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afhængigheder) | [**Schedule**](#schedule)

## Formål
Uploader fil til Ekko App-server med relevante bruger- og ansættelsesdata (synkronisering af brugere).

## Dataflow
Læser ansættelsesdata og persondata for ansatte i Ejendomsservice (baseret på liste af SD-afdelinger) fra SD → genererer en CSV-fil med de relevante data → uploader filen til Ekko apps FTPS-server.


## Beskrivelse
Jobbet henter data fra SD for ansætte i bestemte afdelinger i insitutionen RG og udtrækker de nødvendige data - beskrevet i tabellen her under.

**Beskrivelse af fil der skal uploades til FTPS-server**
| Navn | Personalenr. | Mobiltelefonnr. | Email | MasterGroup | UserGroup | Titel | Fødselsdag | Ansættelsesdato |
|------|--------------|-----------------|-------|-------------|-----------|-------|------------|------------------|
| Her skriver I medarbejderens fulde navn | Fx det der står på lønseddel | Her angiver I medarbejderens mobilnummer | Her angiver I medarbejderens arbejdsemailadresse | Hovedgruppe – primær (geografisk eller organisatorisk) | Undergruppe – afdeling (gerne afdelingsnr.) | Medarbejderens titel | Medarbejderens fødselsdag | Medarbejderens ansættelsesdato |
| Fx Hans Hansen | Fx 7424 | Fx 22334455 | Fx hans.hansen@mitfirma.dk | Timelønnede | Beton | Maskinfører | Fx 31-03-1995 | Fx 31-03-2015 |
| <navn> | <tjeneste- / løn-nummer> | <mobiltelefonnummer> | <emailadresse> | <level 3 parent for SD department > | <sd department > | <titel> | <fødselsdato> | <ansættelsesdato> |

## Afhængigheder

:key: | **Airflow Connections**

**SD API:**
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
  - Connection ID, Host, Login, Password

### Airflow Variables
**Ekko SD-afdelinger**
- **Key:** `ekko_sd_departments`
- **Format (JSON):** `[<id>, <id>, ...]`
- **Beskrivelse:** Liste af ID'er for SD-afdelinger

## Schedule
Schedule er sat op til at køre automatisk på følgende tidspunkter:

- **Tidspunkt:** `0 12 * * *` (hver dag ved middag (12:00))
