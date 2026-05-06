# Modregning Airflow DAG `README.md`
[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afh%C3%A6ngigheder) | [**Schedule**](#schedule)

## Formål

Formålet med jobbet er at hente den nyeste CPR-liste (Excel) fra en SFTP, slå modregningsrelevante ydelser op via Serviceplatform (KOMBIT), og sende en Excel-rapport på email.

## Beskrivelse

Koden består af et DAG-job, der udfører følgende trin:

- Beregner dato-interval ud fra Airflow `logical_date`:
  - `start_dato`: 1. dag i forrige måned
  - `slut_dato`: `logical_date` (dagens dato i DAG’ens timezone)
- Finder nyeste Excel-fil på SFTP (default mønster `*.xlsx`) i den mappe, der er angivet i `modregning_config["sftp_dir"]`
- Læser Excel-arket og udtrækker unikke CPR-numre fra kolonnen `ID-nummer`
  - CPR normaliseres til 10 cifre (ugyldige værdier ignoreres)
  - CPR maskeres i logs (fx `DDMMYYxxxx`)
- Kalder Serviceplatform (SF1491) for hver CPR i dato-intervallet og udtrækker `YdelseNavn`
  - Visse ydelsestyper filtreres fra via `EXCLUDED_YDELSE_NAVNE` listen i koden, hvorved YdelseNavn vil være tom
  - Hvis der ingen ydelser findes i svaret sættes feltet til `Ingen Ydelse`
- Bygger en Excel-rapport (in-memory) med kolonnerne `cpr` og `YdelseNavn`
- Sender rapporten som vedhæftet fil via SMTP (filnavn: `Modregning_YYYY-MM-DD.xlsx`)

**Dataflow:**
- Excel på SFTP → CPR-liste → Serviceplatform-opslag → Excel-rapport → Email

## Afhængigheder

:key: | **Airflow Connections**

**SFTP:**
- **`shared_sftp`**

**Conn Type**: SFTP

Bruges som `Connection id` i Airflow til at hente host, user, pass og port til SFTP’en.

*Required felter*:
  - Connection id, Host, Username, Password og Port(22)

:key: | **Airflow Variables**

**Modregning konfiguration (SFTP + email):**
- **Key**: `modregning_config`

*Required felter*:
  - `sftp_dir` (remote mappe på SFTP)
  - `sender_email`
  - `recipient_emails`
  - `smtp_server`

Eksempel:
```json
{
  "sftp_dir": "/Modregning/",
  "sender_email": "no-reply@randers.dk",
  "recipient_emails": ["modtager1@randers.dk", "modtager2@randers.dk"],
  "smtp_server": "smtp.example.local"
}