# Vognpark Airflow DAG `README.md`
[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afh%C3%A6ngigheder) | [**Schedule**](#schedule)

## Formål

Formålet med jobbet er at sammenholde køretøjsdata fra Insubiz med den nyeste Motorstyrelsen Excel-vedhæftning fra en postkasse, sende en afvigelsesrapport på email, samt gemme et komplet Insubiz-udtræk i PostgreSQL til videre brug(Vognpark-Dashboard)

## Beskrivelse

Kode består af et DAG-job, der udfører følgende trin:

- Henter alle køretøjer fra Insubiz API.

- Henter alle kunder fra Insubiz API og beriger køretøjer med Level1 til Level6 baseret på kundehierarki.

- Finder nyeste ulæste Motorstyrelsen Excel-vedhæftning i Vognpark IMAP postkassen.

- Læser Motorstyrelsen Excel og sammenligner mod Insubiz køretøjer med Afg.dato lig 1900-01-01 ---> Det vil sige aktive køretøjer

- Danner to afvigelseslister:
  - `Skal slettes`: Køretøj findes i Insubiz men ikke i Motorstyrelsen.
  - `Skal tilføjes`: Køretøj findes i Motorstyrelsen men ikke i Insubiz.
  - Filtrerer registreringsnumre i `Skal slettes` til gyldigt format på 1 til 7 tegn.
  - Genererer en Excel-rapport med fanerne `Skal slettes` og `Skal tilføjes`.
  - Sender rapporten via SMTP til konfigurerede modtagere.

- Gemmer Insubiz datasæt i tabellen `vognpark_data`(Til Vognpark Dashboard)

- Gemmer report_date i vognpark_run_audit(Til Vognpark Dashboard)

**Dataflow:**
- Motorstyrelsen + Insubiz sammenligning Flow:
  - Insubiz API → Motorstyrelsen email vedhæftning (IMAP) → sammenligning → Excel rapport → Email

- Insubiz Data flow:
  - Insubiz APi → PostgreSQL tabel vognpark_data


**Bemærk:**

- Jobbet bruger criteria UNSEEN ved søgning efter input mail, så der forventes en ulæst matchende email for at kunne gennemføre.

- Hvis der ikke findes en relevant vedhæftning, fejler jobbet med AirflowFailException.

## Afhængigheder

:key: | **Airflow Connections**

**Postgres DB:**
- **`vognpark_db`**

**Conn Type**: Postgres

 Bruges som `Connection id` i Airflow til at hente host, database, user, pass og port til Postgres DB'en

 *Required felter*:
  - Connection id, Host, Database, Login, Password and Port(5432)

**IMAP (Postkasse til Vognpark):**
- **`vognpark_imap`**
- **Bitwarden navn: `Postkasse - Vognpark`**

Bruges til at hente login/password til Vognpark postkassen, som DAG’en læser input fra.

*Required felter*:
  - Connection id, Username(Login) og Password


**Insubiz API :**
- **`insubiz_cloud_api`**
- **Bitwarden navn: `Insubiz Cloud API`**

**Conn Type**: HTTP

Bruges som Connection id i Airflow til at hente host og Extras(api_key + secret_key) til MP API'et

*Required felter*:
  - Connection id, Host, og Extras
  - f.eks under Extras: { "api_key": x, "secret_key": "y" }


### Airflow Variables 
:key: | **Airflow Variables**

**Vognpark Runtime Konfiguration (email + SMTP ):**
- **Key**: `vognpark_runtime_config`

*Required felter*:
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

Schedule er sat op til at køre automatisk på følgende tidspunkter:

- **Tidspunkt:** Kl. 00:00 hver mandag
- **Cron syntax:**  
  ```
  0 0 * * 1
  ```