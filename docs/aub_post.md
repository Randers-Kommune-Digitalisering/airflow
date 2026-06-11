# AUB Post Airflow DAG `README.md`
[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afhængigheder) | [**Schedule**](#schedule)

## Formål

Formålet med jobbet er at læse emails fra en postkasse, udtrække uddannelse fra vedhæftet PDF (`maindoc.pdf`), sende dokumentet videre til korrekt kontaktperson og slette den oprindelige email efter succesfuld afsendelse.

## Beskrivelse

Koden består af et DAG-job, der udfører følgende trin:

1. Læser emails fra en mailbox i `INBOX` med søgekriteriet `ALL` via rk-digi `EmailReader`.
2. Finder attachment `maindoc.pdf` i hver email.
3. Læser PDF-indhold med PyMuPDF.
4. Finder uddannelse med regex:
   - `Uddannelse\s*\n(.*)`
5. Mapper uddannelse til kontaktperson-email via Airflow Variable.
6. Sender email med vedhæftet `maindoc.pdf` til kontaktpersonen via rk-digi `EmailSender`.
7. Sletter original email med `delete_email_by_uid(..., expunge=True)` efter succesfuld afsendelse.

Hvis et eller flere trin fejler for en email, bliver den ikke slettet, og tasken fejler til sidst for at trigge Airflow retry.

**Dataflow:**
- Email fra postkasse -> PDF udtræk -> uddannelsesmatch -> email routing -> sletning af original email.

## Afhængigheder

:key: | **Airflow Connections**

**Postkasse (IMAP):**
- **`aub_post_imap`**
# TODO: Mangler i bitwarden

**Conn Type**: IMAP

Bruges i rk-digi `EmailReader` til at hente host, login, password og port.

*Required felter*:
- Connection id, Host, Login, Password og Port

### Airflow Variables

**AUB runtime config**
- **Key:** `aub_post_config`
- **Format (JSON):**

```json
{
  "smtp_server": "smtp.example.com",
  "sender_email": "no-reply@randers.dk",
  "mailbox": "INBOX",
  "mail_search_criteria": "ALL",
  "contacts_map": [
    {
      "email": "kontakt1@randers.dk",
      "educations": [
        "Social- og sundhedshjælper",
        "Social- og sundhedsassistent"
      ]
    },
    {
      "email": "kontakt2@randers.dk",
      "educations": [
        "Pædagog"
      ]
    }
  ]
}
```

## Schedule

Jobbet er sat op til at køre automatisk på følgende tidspunkter:

- **Schedule:** `@daily`
- **Catchup:** `false`
- **max_active_runs:** `1`
