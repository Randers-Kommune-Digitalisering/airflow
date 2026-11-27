# eIndkomst Airflow DAG
[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afhængigheder) | [**Schedule**](#schedule)

## Formål

Formålet med jobbet er at danne en Excel-rapport med indkomstoplysninger for CPR- og SE-numre fra en tekstfil. Jobbet henter oplysninger fra SKAT via Serviceplatformen (SKAT Forward eIndkomst, [SF0770A](https://digitaliseringskataloget.dk/integration/sf0770a)) og sender rapporten som vedhæftet fil.

## Beskrivelse

Koden består af et DAG-job, der udfører følgende trin:
- Finder nyeste `.txt`-vedhæftning i postkassen, hvor filnavnet starter med `eindkomst`.
- Læser en semikolonsepareret tekstfil med header på første linje. Headeren skal indeholde `CPR-NUMMER` og  `SE-NR`-kolonne. En beskrivelsesrække på linje 2 uden værdi i `CPR-NUMMER` springes over tomme linjer og øvrige kolonner ignoreres.
- Validerer CPR-numre (10 cifre uden bindestreg) og behandler hvert unikt CPR/SE-NR-par én gang. Filen skal være UTF-8, andre encodings giver fejl.
- Læser `start_month` og `end_month` fra DAG-parametrene som et månedsinterval i formatet `YYYYMM`.
- Slår hvert CPR op via SF0770A og medtager kun indkomstperioder, hvor SE-nummeret i svaret matcher inputrækkens `SE-NR`. Hvis `SE-NR` er tomt, medtages CPR med tomme indkomstfelter uden opslag.
- Danner en Excel-rapport med arket `Indkomstoplysninger` med tilhørende fælles felter, CPR, SE-nummer, tjenestenummer, dispositionsdato, angivelsesperiode og de konfigurerede SKAT-felter. 
- Sender rapporten via SMTP med filnavnet `eIndkomst_<STARTMÅNED>_til_<SLUTMÅNED>_<YYYY-MM-DD>.xlsx`.



**Dataflow:**

- Eindkomst Postkasse (tekstfil(txt) med CPR- og SE-numre) -> opslag via SF0770A -> Excel-rapport (`Indkomstoplysninger`) -> e-mail til de konfigurerede modtagere.

**Bemærk (datahåndtering):**

Inputmailen slettes fra Eindkomst Personalehr postkassen efter behandlingsforsøget, også hvis rapportdannelsen eller afsendelsen fejler.

**Forudsætning (manuel proces):**

Læg en semikolonsepareret `.txt`-fil i Eindkomst Personalehr postkassen med et filnavn, der starter med `eindkomst`. Filen skal have `CPR-NUMMER` og `SE-NR` i headeren. Den, der starter DAG'en, skal angive både `start_month` og `end_month`.

## Afhængigheder

### Kombit_client (Serviceplatformen)

Jobbet anvender `kombit_client` til SF0770A. Klientcertifikatet leveres via `TempClientCert` og kræver `CLIENT_CERT_PUBLIC_KEY` og `CLIENT_CERT_PRIVATE_KEY`. De øvrige certifikater fra Serviceplatformen ligger i `dags/certs`.

### Airflow Connections
:key: | **Airflow Connections**

**IMAP (eIndkomst personalehr-postkasse):**
- **`eindkomst_imap`**

Jobbet bruger aktuelt denne Connection ID til login og adgangskode til postkassen, der indeholder eIndkomst-input.

*Påkrævede felter:*
	- Connection ID, Username (Login) og Password.

### Airflow Variables
:key: | **Airflow Variables**

**eIndkomst runtime-konfiguration: (e-mail, SMTP og IMAP):**
- **Key:** `eindkomst_runtime_config`

*Påkrævede felter:*
	- `sender_email`
	- `recipient_emails`
	- `smtp_server`
	- `imap_server`

Eksempel:
```json
{
  "sender_email": "no-reply@example.com",
  "recipient_emails": ["modtager@example.com"],
  "smtp_server": "smtp.example.local",
  "imap_server": "imap.example.local"
}
```

**SKAT-klientkonfiguration:**
- **Key:** `skat_client_config`

*Påkrævede felter:*
	- `virksomhed_se_nummer_identifikator`
	- `abonnement_type_kode`
	- `abonnent_type_kode`
	- `adgang_formaal_type_kode`

Eksempel:
```json
{
  "virksomhed_se_nummer_identifikator": "12345678",
  "abonnement_type_kode": "1223",
  "abonnent_type_kode": "54321",
  "adgang_formaal_type_kode": "6767"
}
```

**Rapportkonfiguration (kolonner og indkomstfelter):**
- **Key:** `eindkomst_report_config`

*Påkrævede felter:*
	- `report_columns`: Kolonnerne i den ønskede rækkefølge.
	- `report_column_to_field_name`: Beskrivelse for hver konfigureret `Felt`-kolonne.
	- `report_field_to_blanket_field_id`: SKAT-blanketfelt-id for hver feltbeskrivelse. 

Eksempel:
```json
{
  "report_columns": [
    "CPR",
    "SE-Nummer",
    "Tjenestenummer",
    "IndkomstPersonGruppeDispositionDato",
    "AngivelsePeriode",
    "Felt 13"
  ],
  "report_column_to_field_name": {
    "Felt 13": "A-indkomst (hvoraf AM-bidrag)"
  },
  "report_field_to_blanket_field_id": {
    "A-indkomst (hvoraf AM-bidrag)": "100000000000000057"
  }
}
```

## Schedule

Personale og HR har adgang til UI'en i Airflow med rollen: `Eindkomst`. Her kan de selv trigger DAG'en efter eget behov.

Jobbet `eindkomst_income_report` startes manuelt. Ved start skal følgende parametre angives:

- `start_month`: Første måned i intervallet, `YYYYMM`.
- `end_month`: Sidste måned i intervallet, `YYYYMM`.

Eksempel:
- `start_month`: `202604`
- `end_month`: `202606`

**Nyttige docs:**
* [eIndkomst Udstilling](https://info.skat.dk/data.aspx?oid=2248828&chk=220344) (underbilag 1 beskriver blanketter og felter)
* [Kombit Client](https://pypi.org/project/kombit-client/)
