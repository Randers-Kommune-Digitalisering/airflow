# Arbejdsfortjeneste Airflow DAG `README.md`
[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afhængigheder) | [**Schedule**](#schedule)

## Formål

Formålet med jobbet er at understøtte kontrollen af personer, der er tildelt tabt arbejdsfortjeneste, ved at identificere ændringer i deres indkomst. Jobbet læser den nyeste relevante CPR-liste fra Arbejdsfortjenestse -  Postkassen, henter indkomstoplysninger via Serviceplatformen (SKAT Forward eIndkomst, [SF0770A](https://digitaliseringskataloget.dk/integration/sf0770a)) og sender en rapport til Familie- og rådgivningscenter.

## Beskrivelse

Koden består af et DAG-job, der udfører følgende trin:

- Finder nyeste Excel-vedhæftning i postkassen, hvor:
	- filtype matcher `.xlsx`
	- filnavn starter med `Liste`
- Læser Excel og udtrækker CPR-numre fra kolonnen `Cprnr.`:
	- CPR normaliseres ved at fjerne bindestreg
- Læser `start_month` og `end_month` fra DAG params:
	- `start_month` (format `YYYYMM`)
	- `end_month` (format `YYYYMM`)
- Validerer at begge parametre er angivet
- Kalder Serviceplatformen for hver CPR:
	- En samlet forespørgsel for hele intervallet
	- En forespørgsel pr. måned i intervallet til ændringsberegning
	- Hvis et CPR-opslag fejler, indsættes en placeholder-række, så CPR stadig fremgår i rapporten
- Henter indkomst fra Serviceplatform-svaret, inkl. felter mappet fra blanket-felt-id'er.
- Bygger månedlige differencer:
	- Aggregerer numeriske felter pr. nøglekolonner (default `cpr`)
	- Beregner ændring mellem forrige og nuværende måned
	- Sætter indikator:
		- `▲` ved positiv ændring
		- `▼` ved negativ ændring
		- `-` ved ingen ændring
- Genererer Excel-rapport med 2 ark:
	- `Indkomstoplysninger`
	- `Ændring` (kun rækker med ændring forskellig fra 0)
- Sender rapporten som vedhæftet fil via SMTP.
- Filnavn følger formatet:
	- `Arbejdsfortjeneste_<STARTMÅNED>_til_<SLUTMÅNED>_<YYYY-MM-DD>.xlsx`

**Dataflow:**
- Arbejdsfortjeneste Postkasse Email (Excel vedhæftning) -> CPR-liste -> Serviceplatform-opslag ([SF0770A](https://digitaliseringskataloget.dk/integration/sf0770a)) -> Excel-rapport (Indkomstoplysninger + Ændring) -> Email til Familie- og rådgivningscenter

Bemærk (datahåndtering):

Når rapporten er sendt, slettes den behandlede email (med CPR-listen som vedhæftning) fra Arbejdsfortjeneste postkassen for at minimere unødig opbevaring af inputdata.


**Forudsætning (manuel proces):**

Familie- og rådgivningscenter lægger en CPR-liste i Arbejdsfortjeneste - postkassen som Excel-vedhæftning.
Excel-filen skal indeholde kolonnen `Cprnr.`.
Filnavnet skal starte med `Liste` for at blive fundet af jobbet.
Bruger, der trigger DAG'en, skal angive både `start_month` og `end_month`.

## Afhængigheder

### Kombit_client (Serviceplatformen)

Da koden anvender kombit_client pakken kræver det at man sætter CLIENT_CERT_PUBLIC_KEY og CLIENT_CERT_PRIVATE_KEY. De resterende certifikater fra Serviceplatformen ligger i mappen `cert`

### Airflow Connections
:key: | **Airflow Connections**

**IMAP (Postkasse til Arbejdsfortjeneste):**
- **`arbejdsfortjeneste_imap`**
- **Bitwarden navn: `Postkasse - Arbejdsfortjeneste`**

Bruges til at hente login/password til Arbejdsfortjeneste postkassen, som DAG'en læser input fra.

*Required felter*:
	- Connection id, Username (Login) og Password

### Airflow Variables
:key: | **Airflow Variables**

**Arbejdsfortjeneste Runtime Konfiguration (email + SMTP + IMAP):**
- **Key**: `arbejdsfortjeneste_runtime_config`

*Required felter*:
	- `sender_email`
	- `recipient_emails`
	- `smtp_server`
	- `imap_server`

Eksempel:
```json
{
	"sender_email": "no-reply@randers.dk",
	"recipient_emails": ["modtager1@randers.dk", "modtager2@randers.dk"],
	"smtp_server": "smtp.example.local",
	"imap_server": "imap.example.local"
}
```

**SKAT klientkonfiguration:**
- **Key**: `skat_client_config`

*Required felter*:
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

**Rapportkonfiguration (felter, mappings, ændringsnoegler):**
- **Key**: `arbejdsfortjeneste_report_config`

*Required felter*:
	- `income_type_code_to_label`
	- `report_field_to_blanket_field_id`
	- `required_fields_from_blanket_16001`
	- `change_report_key_columns`
	- `change_report_numeric_fields`

Eksempel:
```json
{
	"income_type_code_to_label": {
		"0": "Lønansat",
		"1": "SU"
	},
	"report_field_to_blanket_field_id": {
		"Lontimer": "100000000000000096",
		"IndkomstType": "100000000000000012"
	},
	"required_fields_from_blanket_16001": [
		"Løntimer"
	],
	"change_report_key_columns": [
		"cpr"
	],
	"change_report_numeric_fields": [
		"A-indkomst (hvoraf AM-bidrag)"
	]
}
```

## Schedule

Familie- og rådgivningscenter har adgang til UI'en i Airflow med rollen: `Arbejdsfortjeneste` hvor de kun kan se det DAG som tilhører Arbejdsfortjeneste. Her kan de selv trigger DAG'et efter eget behov. Når man trigger jobbet skal man angive følgende parameter:

- `start_month`: `YYYYMM`
- `end_month`: `YYYYMM`

Eksempel:
- `start_month`: `202604`
- `end_month`: `202606`


**Nyttige docs:**
* [eIndkomst Udstilling](https://info.skat.dk/data.aspx?oid=2248828&chk=220344) (`underbilag 1 (excel`) Beskriver de forskellige blanketter og felter)