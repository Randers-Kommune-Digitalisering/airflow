# Vognpark Samnmenligne Motorstyrelsen VS Insubiz 
[Formål](#formål) | [Beskrivelse](#beskrivelse) | [Inputkrav](#inputkrav) | [Output](#output) | [Afhængigheder](#afhængigheder) | [Schedule](#schedule)

## Formål

Formålet med denne DAG er sammenligning mellem Motorstyrelsen PDF og Insubiz-data samt sende en mail over køretøjer som skal slettes og tilføjes i Insubiz.

## Beskrivelse

**DAG:** `dag_vognpark_compare_and_report`

Udfører følgende:

- Henter alle køretøjer fra Insubiz API.
- Henter alle kunder fra Insubiz API og beriger køretøjer med `Level1` til `Level6`.
- Finder nyeste ulæste Motorstyrelsen PDF i Vognpark-postkassen.
- Parser PDF og sammenligner registreringsnumre mod Insubiz.
- Sammenligningen sker mod Insubiz-køretøjer med `Afg.dato = 1900-01-01 00:00:00`.
- Genererer en Excel-rapport med afvigelser: `Skal slettes` og `Skal tilføjes` fanerne
- Sender Excel arket via SMTP til Forskringskontoret

## Inputkrav

- Motorstyrelsen-filen skal sendes til `Vognpark-Postkassen`.
- Vedhæftet fil skal være `.pdf` og have filnavn med prefix `maindoc`.
- DAG læser kun ulæste mails (`UNSEEN`).

Hvis der ikke findes en relevant PDF-vedhæftning, fejler DAG med `AirflowFailException`.

## Output

Sender en email med vedhæftet Excel-fil:

- Filnavn: `uoverensstemmelser_biler_YYYY-MM-DD.xlsx`
- Fane `Skal slettes`: køretøjer som findes i Insubiz men ikke i Motorstyrelsen.
- Fane `Skal tilføjes`: køretøjer som findes i Motorstyrelsen men ikke i Insubiz.

## Afhængigheder

### Airflow Connections

**IMAP (Vognpark postkasse)**

- Connection id: `vognpark_imap`
- Bitwarden navn: `Postkasse - Vognpark`
- Krævede felter: Username (Login), Password

**Insubiz API**

- Connection id: `insubiz_cloud_api`
- Bitwarden navn: `Insubiz Cloud API`
- Type: HTTP
- Krævede felter: Host + Extras
- Eksempel på extras:

```json
{
  "api_key": "x",
  "secret_key": "y"
}
```

### Airflow Variables

**Vognpark runtime konfiguration (email + SMTP)**

- Key: `vognpark_runtime_config`
- Krævede felter:
  - `sender_email`
  - `recipient_emails`
  - `smtp_server`

Eksempel:

```json
{
  "sender_email": "no-reply@randers.dk",
  "recipient_emails": ["modtager1@randers.dk", "modtager2@randers.dk"],
  "smtp_server": "smtp.example.localhost"
}
```

## Schedule

Forsikringskontoret har adgang til UI'en i Airflow med rollen: `Vognpark` hvor de kun kan se de 3 DAGS som tilhører Vognpark. Her kan de selv trigger DAG'et
