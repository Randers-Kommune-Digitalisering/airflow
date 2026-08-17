# Modregning Airflow DAG `README.md`
[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afh%C3%A6ngigheder) | [**Schedule**](#schedule)

## Formål

Formålet med jobbet er at understøtte Betalingskontorets behov for at sammenholde personer (fra en CPR-liste) med oplysninger om ydelsesudbetalinger hentet via Serviceplatform pakken: ([kombit_client](https://pypi.org/project/kombit-client/)) i et givent dato-interval. Jobbet henter den nyeste uset CPR-liste (Excel) fra en postkasse, slår relevante ydelsestyper op pr. CPR i Serviceplatformen og genererer en Excel-rapport, som sendes på email. Rapporten bruges som grundlag for kontrol og opfølgning på personer, der modtager bestemte ydelsestyper (herunder at kunne frasortere ydelsestyper via excluded-listen).


## Beskrivelse

Koden består af et DAG-job, der udfører følgende trin:

- Læser `start_date` og `end_date` fra DAG params (valgfrie)
- Hvis begge datoer er angivet ved trigger:
  - `start_date` og `end_date` bruges direkte (format YYYY-MM-DD)
  - Jobbet validerer at `start_date` ikke er efter `end_date`
- Hvis ingen datoer er angivet ved trigger:
  - `start_date` beregnes automatisk som første dag i forrige måned (ud fra logical_date i DAG timezone)
  - `end_date` sættes automatisk til logical_date (i DAG timezone)
- Hvis kun én af datoerne er angivet:
  - Jobbet fejler med valideringsfejl (begge datoer skal angives sammen, eller begge udelades)
- Finder nyeste Excel-vedhæftning i en IMAP Modregning-postkassen (default `INBOX`)
  - Email hentes via IMAP (EmailReader)
  - Jobbet scanner de seneste emails (nyeste først) og leder efter en `.xlsx`-vedhæftning, hvor filnavnet starter med et af de konfigurerede prefixes (fx `Modregning`)
- Læser Excel-arket og udtrækker unikke CPR-numre fra kolonnen `ID-nummer`
  - CPR normaliseres til 10 cifre (ugyldige værdier ignoreres)
- Kalder Serviceplatform (SF1491) for hver CPR i dato-intervallet og udtrækker `YdelseNavn`
  - Visse ydelsestyper filtreres fra via `EXCLUDED_YDELSE_NAVNE` listen i Airflow Variablen `modregning_excluded_ydelse_list`, hvorved YdelseNavn vil være tom
  - Hvis der ingen ydelser findes i svaret sættes feltet til `Ingen Ydelse`
- Bygger en Excel-rapport (in-memory) med kolonnerne `cpr` og `YdelseNavn`
- Sender rapporten som vedhæftet fil via SMTP (filnavn: `Modregning_YYYY-MM-DD.xlsx`)
- Sletter input-emailen fra Modregning-postkassen uanset vellykket gennemførsel eller hvis der er en fejl i Excel-arket(Mangler `ID-nummer`).
  - Emailen slettes via UID i `INBOX` og expunges med det samme. Det vil sige at input mailen hverken kan findes under
  `INBOX` eller `Deleted Items`. Den bliver slettet permanent.

**Dataflow:**
- Modregning Postkasse Email (IMAP) + Excel vedhæftning → CPR-liste → Serviceplatform-opslag → Excel-rapport → Email

**Bemærk (datahåndtering):**
- Når rapporten er sendt, slettes den behandlede email (med CPR-listen som vedhæftning) fra postkassen for at minimere unødig opbevaring af inputdata.

**Forudsætning(manuel proces):**

Betalingskontoret sender en ny CPR-liste (Excel) til Modregning Postkassen. CPR-listen bruges som input til modregningsopslag.
Excel-filen skal indeholde kolonnen `ID-nummer` (CPR). 
Jobbet bruger den nyeste matchende vedhæftning i postkassen, hvis der ikke ligger en relevant mail med vedhæftet Excel, kan jobbet ikke gennemføre rapporten som forventet. Betalingskontoret vedligeholder desuden listen `modregning_excluded_ydelse_list` (tilføj/fjern ydelser efter behov). 

- Brugeren, der trigger DAG'en/jobbet, kan vælge enten:
  - Angive både `start_date` og `end_date` manuelt
  - Lade begge felter være tomme, så dato-intervallet beregnes automatisk

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

**Modregning Runtime Konfiguration (email + SMTP ):**
- **Key**: `modregning_runtime_config`

*Required felter*:
  - `sender_email`
  - `recipient_emails`
  - `smtp_server`
  - 

Eksempel:
```json
{
  "sender_email": "no-reply@randers.dk",
  "recipient_emails": ["modtager1@randers.dk", "modtager2@randers.dk"],
  "smtp_server": "smtp.example.local",
  "imap_server": "imap.example.local"
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

## Schedule

Betalingskontoret har adgang til UI'en i Airflow med rollen: `Modregning` hvor de kun kan se det DAG som tilhører Modregning. Her kan de selv trigger DAG'et efter eget behov. Når man trigger jobbet, er dato-parametre valgfrie.

Mulighed 1 (manuel periode):

- `start_date`: `YYYY-MM-DD`
- `end_date`: `YYYY-MM-DD`

Eksempel:
- `start_date`: `2026-06-01`
- `end_date`: `2026-07-27`


Mulighed 2 (automatisk periode):

- `start_date`: `tom`
- `end_date`: `tom`


Automatisk beregning:
- `start_date` = første dag i forrige måned (relativt til logical_date)
- `end_date` = logical_date
