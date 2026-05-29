# Modregning Airflow DAG `README.md`
[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afh%C3%A6ngigheder) | [**Schedule**](#schedule)

## Formål

Formålet med jobbet er at understøtte Betalingskontorets behov for at sammenholde personer (fra en CPR-liste) med oplysninger om ydelsesudbetalinger hentet via Serviceplatform pakken: ([kombit_client](https://pypi.org/project/kombit-client/)) i et givent dato-interval. Jobbet henter den nyeste CPR-liste (Excel) fra en postkasse, slår relevante ydelsestyper op pr. CPR i Serviceplatformen og genererer en Excel-rapport, som sendes på email. Rapporten bruges som grundlag for kontrol og opfølgning på personer, der modtager bestemte ydelsestyper (herunder at kunne frasortere ydelsestyper via excluded-listen).


## Beskrivelse

Koden består af et DAG-job, der udfører følgende trin:

- Beregner dato-interval ud fra Airflow `logical_date`:
  - `start_dato`: 1. dag i forrige måned
  - `slut_dato`: `logical_date` (dagens dato i DAG’ens timezone)
- Finder nyeste Excel-vedhæftning i en IMAP Modregning-postkassen (default `INBOX`)
  - Email hentes via IMAP (EmailReader)
  - Jobbet scanner de seneste emails (nyeste først) og leder efter en `.xlsx`-vedhæftning, hvor filnavnet starter med et af de konfigurerede prefixes (fx `Modregning` eller `DAKT`)
- Læser Excel-arket og udtrækker unikke CPR-numre fra kolonnen `ID-nummer`
  - CPR normaliseres til 10 cifre (ugyldige værdier ignoreres)
- Kalder Serviceplatform (SF1491) for hver CPR i dato-intervallet og udtrækker `YdelseNavn`
  - Visse ydelsestyper filtreres fra via `EXCLUDED_YDELSE_NAVNE` listen i koden, hvorved YdelseNavn vil være tom
  - Hvis der ingen ydelser findes i svaret sættes feltet til `Ingen Ydelse`
- Bygger en Excel-rapport (in-memory) med kolonnerne `cpr` og `YdelseNavn`
- Sender rapporten som vedhæftet fil via SMTP (filnavn: `Modregning_YYYY-MM-DD.xlsx`)
- Sletter input-emailen fra IMAP-postkassen efter vellykket gennemførsel (rapport sendt)
  - Emailen slettes via UID i `INBOX` og expunges med det samme. Det vil sige at input mailen hverken kan findes under
  `INBOX` eller `Deleted Items`. Den bliver slettet permanent

**Dataflow:**
- Modregning Postkasse Email (IMAP) + Excel vedhæftning → CPR-liste → Serviceplatform-opslag → Excel-rapport → Email

**Bemærk (datahåndtering):**
- Når rapporten er sendt, slettes den behandlede email (med CPR-listen som vedhæftning) fra postkassen for at minimere unødig opbevaring af inputdata.

**Forudsætning(manuel proces):**

Den 15. i hver måned sender Betalingskontoret en ny CPR-liste (Excel) til Modregning Postkassen. CPR-listen bruges eom input til modregningsopslag.
Excel-filen skal indeholde kolonnen `ID-nummer` (CPR). 
Jobbet bruger den nyeste matchende vedhæftning i postkassen, hvis der ikke ligger en relevant mail med vedhæftet Excel, kan jobbet ikke gennemføre rapporten som forventet. Betalingskontoret vedligeholder desuden listen `modregning_excluded_ydelse_list` (tilføj/fjern ydelser efter behov).

## Afhængigheder

### Kombit_client(Serviceplatformen)

Da koden anvender kombit_client pakken kræver det at man sætter **`CLIENT_CERT_PUBLIC_KEY`** og **`CLIENT_CERT_PRIVATE_KEY`**. De resterende certifikater fra Serviceplatformen ligger i mappen **`Certificates`**


### Airflow Connections
:key: | **Airflow Connections**

**IMAP (Postkasse til Modregning):**
- **`modregning_imap`**

Bruges til at hente login/password til Modregning postkassen, som DAG’en læser input fra.

*Required felter*:
  - Connection id, Username(Login) og Password


### Airflow Variables 
:key: | **Airflow Variables**

**Modregning Runtime Konfiguration (SFTP + email + SMTP ):**
- **Key**: `modregning_runtime_config`

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
```

**Modregning Excluded Ydelse navne liste:**
- **Key**: `modregning_excluded_ydelse_list`

*Required felter*:
  - `excluded_ydelse_name` (Værdier fra ydelser som skal excludes fra udtrækket)

Eksempel:
```json
{
  "excluded_ydelse_name": [
    "Sygedagpenge til virksomhed",
    "Sygedagpenge til borger"
  ]
}
```

## Schedule og dato interval


DAG’en er planlagt til at køre kl. 09:00 den 15. i hver måned. For hver kørsel beregnes dato-intervallet ud fra Airflows `logical_date `(konverteret til DAG’ens timezone og trunkeret til dato):

- `slut_dato` = datoen for `logical_date`
- `start_dato` = 1. dag i måneden før `logical_date`


Det betyder, at en planlagt kørsel den 15. i måneden typisk dækker perioden fra 1. i forrige måned til 15. i indeværende måned (inkl.).

**Eksempel:**

Kørsel: 2026-05-15 kl. 09:00 --->  `logical_date` = 2026-05-15
- `start_dato` = 2026-04-01
- `slut_dato` = 2026-05-15