# Socialt Frikort Airflow DAG `README.md`
[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afhængigheder) | [**Schedule**](#schedule)

## Formål

Formålet med jobbet er at danne en Excel-rapport, der kan bruges som grundlag for at vurdere, om en borger er berettiget til et socialt frikort. Jobbet læser en CPR-liste fra Socialt Frikort-postkassen, henter indkomstoplysninger via Serviceplatformen (SKAT Forward eIndkomst, [SF0770A](https://digitaliseringskataloget.dk/integration/sf0770a)), slår aktuelle og historiske bopælsadresser op via [SF1520](https://digitaliseringskataloget.dk/integration/sf1520) og sender rapporten som Excel-fil.

## Beskrivelse

Koden består af et DAG-job, der udfører følgende trin:
- Finder nyeste ulæste Excel-vedhæftning i postkassen, hvor
  - filtype matcher `.xlsx`
  - filnavn starter med `Social`
- Læser Excel og udtrækker CPR-numre fra kolonnen `Cprnr.`.
- Læser `start_month` og `end_month` fra DAG-parametrene og validerer, at begge er angivet i formatet `YYYYMM`, og at startmåned ikke ligger efter slutmåned.
- Foretager for hvert CPR:
	- Ét indkomstopslag for hele det angivne månedsinterval via SF0770A.
	- Ét adresseopslag via SF1520.
- Danner en Excel-rapport med to ark:
	- `Indkomstoplysninger`: Én række pr. CPR med `CPR`, `Søgeperiode` (fx `202601 - 202610`) og `Samlet Indtægt`. Beløbet er summen af de to A-indkomstfelter på tværs af hele søgeperioden. Manglende indkomst giver 0; hvis SKAT-opslaget fejler, er beløbet tomt.
	- `Adresse Historik`: Aktuel adresse efterfulgt af eventuelle historiske adresser pr. CPR.
- Sender rapporten som vedhæftet fil via SMTP. Filnavnet følger formatet `SocialtFrikort_<STARTMÅNED>_til_<SLUTMÅNED>_<YYYY-MM-DD>.xlsx`.

Arket `Adresse Historik` har kolonnerne `CPR`, `AdresseType`, `StandardAdresse`, `Postnummer`, `Vejnavn`, `Husnummer`, `Tilflytningsdato`, `Fraflytningsdato`, `Etage`, `Sidedoer` og `Postdistrikt` 

**Dataflow:**

- Socialt Frikort-postkasse (Excel med CPR-liste) -> opslag via SF0770A og SF1520 -> Excel-rapport (`Indkomstoplysninger` og `Adresse Historik`) -> e-mail til de konfigurerede modtagere.

**Bemærk (datahåndtering):**

Når rapporten er sendt, slettes den behandlede email (med CPR-listen som vedhæftning) fra Social Frikort postkassen for at minimere unødig opbevaring af inputdata.

**Forudsætning (manuel proces):**

Læg en `.xlsx`-fil med kolonnen `Cprnr.` i Socialt Frikort-postkassen. Filnavnet skal starte med `Social`. Bruger, der trigger DAG'en, skal angive både `start_month` og `end_month`.

## Afhængigheder

### Kombit_client (Serviceplatformen)

Jobbet anvender `kombit_client` til SF0770A og SF1520. Jobbet kræver det at man sætter `CLIENT_CERT_PUBLIC_KEY` og `CLIENT_CERT_PRIVATE_KEY`. De resterende certifikater fra Serviceplatformen ligger i mappen cert

### Airflow Connections
:key: | **Airflow Connections**

**IMAP (Socialt Frikort-postkasse):**
- **`social_frikort_imap`**

Bruges til at hente login og adgangskode til postkassen, som DAG'en læser input fra.

*Påkrævede felter:*
	- Connection ID, Username (Login) og Password.

### Airflow Variables
:key: | **Airflow Variables**

**Socialt Frikort runtimekonfiguration (e-mail, SMTP og IMAP):**
- **Key:** `social_frikort_runtime_config`

*Påkrævede felter:*
	- `sender_email`
	- `recipient_emails`
	- `smtp_server`
	- `imap_server`

Eksempel:
```json
{
	"sender_email": "no-reply@randers.dk",
	"recipient_emails": ["modtager@randers.dk"],
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

**Rapportkonfiguration (indkomstfelter):**
- **Key:** `social_frikort_report_config`

*Påkrævede felter:*
	- `report_field_to_blanket_field_id` med præcis to forskellige SKAT-felt-id'er til de to A-indkomstfelter. Begge beløb summeres til `Samlet Indtægt`.

Eksempel:
```json
{
	"report_field_to_blanket_field_id": {
		"A-indkomst (hvoraf AM-bidrag)": "100000000000000057",
		"A-indkomst (hvoraf ikke AM-bidrag)": "100000000000000058"
	}
}
```

## Schedule

Jobbet `social_frikort_indkomst_og_adresse_report` startes manuelt. Afdelingen har adgang til UI'en i Airflow med rollen `Social`, hvor de kun kan se det DAG som tilhører Social Frikort. Ved start skal følgende parametre angives:

- `start_month`: Første måned i intervallet, `YYYYMM`.
- `end_month`: Sidste måned i intervallet, `YYYYMM`.

Eksempel:
- `start_month`: `202604`
- `end_month`: `202606`

**Nyttige docs:**
* [eIndkomst Udstilling](https://info.skat.dk/data.aspx?oid=2248828&chk=220344) (`underbilag 1 (excel`) Beskriver de forskellige blanketter og felter)
* [Kombit Client](https://pypi.org/project/kombit-client/)