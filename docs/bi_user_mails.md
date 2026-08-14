# BI user mail onboarding Airflow DAG `README.md`

[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) | [**Schedule**](#schedule)

## Formål

Formålet med jobbet er at automatisere udsendingen af onboarding mails til nye BI brugere.

Jobbet henter en .xlsx-fil fra ekstern server via sftp. Læser indholdet af brugere og sammenligner med en allerede kendt liste af brugere gemt i databasen (pgsql). Hvis en ny bruger opdages, dvs. en bruger der endnu ikke er listet i databasen, udsendes der en mail til vedkommende, hvorefter det markeres i databasen at der nu er afsendt en mail til vedkommende.

Som udgangspunkt sendes der en mail til hver ny bruger. Men hvis en bruger er tilknyttet en anden gruppe end "Web statistik bruger" sendes der ligeledes en mail til support.

## Beskrivelse

Koden består af et DAG-job, der udfører følgende trin:

- Læs .xlsx fil fra sftp-server
  - Sti til fil: /CustomData/Uddata/
  - Filnavn: Nye og inaktive brugere.xlsx

- Læser xlsx-filen og finder oplysninger om nye brugere:
  - `oprettelses dato`
  - `bruger navn`
  - `bruger id (DQ)`
  - `email adresse`
  - `bruger gruppe navn`
- Ovenstående bruger data indsættes i database med email adresse som nøgle:
- Hvis brugeren ikke allerede eksisterer, oprettes den
- Hvis brugeren er ny eller står som kendt, men med email_sent = false, afsendes onboarding email
- Når email er afsendt, markeres dette i databasen

**Email ved fejl:**

N/A

### Airflow Connections

**Database credentials til airflow-external-postgresql:**
 - id: bi_user_mail_db

*Required felter*:
- Connection id
- Host
- Database
- Login
- Password
- Port

**SFTP credentials til ekstern SFTP-server:**
- id: intftp_kmd

*Required felter*:
- Connection id
- Host
- Username
- Password

### Airflow Variables

**BI User Mail Runtime Configuration:**
- **Key**: `bi_user_mail_runtime_config`

*Required felter*:
  - `smtp_server`
  - `sender_email`
  - `smtp_port`
  - `user_group_default`
  - `bi_contact_list`
  - `sftp_file_path`

Eksempel:
```json
{
"sender_email":"digitalisering@randers.dk",
"smtp_server":"smtp.randers.dk",
"smtp_port":"25",
"user_group_default":"Web statistik bruger",
"bi_contact_list":["Navn A <a@test.dk>", "Navn B <b@test.dk>"],
"sftp_file_path":"/CustomData/Uddata/Nye og inaktive brugere.xlsx"
}
```

## Schedule

Jobbet kører alle hverdage (mandag - fredag) kl. 09:00.

Beskrevet med følgende cron expression: "0 9 * * 1-5"
