# Kantinedata Airflow DAG README

[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afhængigheder) | [**Schedule**](#schedule)

## Formål

Formålet med jobbet er at hente Kantinedata fra e-mail (vedhæftede filer) og uploade relevante XML-filer til en SFTP-server i et fast filnavnsformat, så modtagersystemet kan hente filerne.

Jobbet er robust overfor midlertidige fejl ved at markere e-mails under behandling med et flag og automatisk genprøve mislykkede e-mails i efterfølgende runs.

Automatiseringen antager at filer jævnligt slettes fra SFTP-serveren efter behandling af disse.

## Beskrivelse

Koden består af et DAG-job der kører `process_kantinedata`, som udfører følgende trin:

- Forbinder til en IMAP-mailkonto via Airflow Connection `kantinedata_imap`.
- Henter e-mails i INBOX i to pass:
	- **FLAGGED**: e-mails der tidligere var under behandling, men hvor et run fejlede (retry-kø).
	- **UNSEEN**: nye e-mails; disse flagges med `\\Flagged` ved hentning for at markere at de er “under behandling”.
- Kombinerer de to lister og undgår dubletter baseret på **UID**.
- For hver e-mail:
	- Vedhæftninger kontrolleres.
	- Hvis der **ingen vedhæftninger** er, markeres e-mailen som succesfuldt behandlet (for at undgå gentagen behandling).
	- For hver vedhæftning:
		- **Uploader kun XML** (`application/xml` eller `text/xml`) til SFTP.
		- Ikke-XML vedhæftninger springes over.
- Efter behandling:
	- E-mails der er behandlet succesfuldt **unflagges** (fjerner `\\Flagged`).
	- Hvis én eller flere e-mails fejler, kastes en exception til sidst, så Airflow kan retry’e tasken.

### Filnavne og rotation (SFTP)

XML-vedhæftninger uploades til SFTP med et roterende filnavn:

- Format: `EksporteredeOrdrer_<n>.xml` hvor `<n>` er 1..10
- Et løbenummer gemmes i Airflow Variable `kantinedata_file_counter`
- Ved upload allokeres næste nummer (wrapper efter 10)
- Der checkes samtidig på SFTP for kollisioner (hvis en fil allerede findes), og der vælges i så fald næste ledige slot
- Hvis alle 10 slots allerede eksisterer på SFTP, fejler jobbet med en klar fejl

Filerne navngives således for at understøtte nem indlæsning i FME-software.

### Drift / fejlhåndtering

- **Retry-mekanisme via e-mail flagging**:
	- Nye e-mails (UNSEEN) flagges under behandling.
	- Ved fejl forbliver de flaggede, og næste run vil hente dem via kriteriet `FLAGGED`.
- Hvis en e-mail ikke kan behandles (fx parsingfejl, SFTP-fejl), fortsætter jobbet med øvrige e-mails, men der kastes en samlet fejl til sidst for at trigge retry.
- SFTP-forbindelsen lukkes til sidst (best effort) for at undgå hængende forbindelser.

**Dataflow:**

- E-mail (IMAP INBOX) → Vedhæftningsudtræk → Filtrering (XML) → Upload til SFTP

## Afhængigheder

:key: | **Airflow Connections**

**IMAP (e-mail):**
- **`kantinedata_imap`**
	Bruges til at læse e-mails fra INBOX.

	*Forventede felter:*
	- Login (email)
	- Password
	- Host
	- Port

**SFTP:**
- **`kantinedata_sftp`**
	Bruges til at uploade XML-filer.

	*Forventede felter:*
	- Host
	- Username
	- Password
	- Port

:key: | **Airflow Variables**

- **`kantinedata_file_counter`**
	Bruges som løbenummer for filnavne-rotationen (1..10). Hvis den mangler eller har ugyldig værdi, starter jobbet fra 1. Værdien oprettes automatisk, og tælles automatisk op for hver fil som uploades til SFTP.

:key: | **Python dependencies**

- Airflow med SFTP provider (`airflow.providers.sftp`) for `SFTPHook`.
- `rkdigi.email_handling.EmailReader` til IMAP-læsning.

## Schedule

DAG’et er sat op til at køre automatisk på følgende tidspunkter:

- **Schedule:** `@daily`
- **Start date:** 2026-01-16 (Europe/Copenhagen)
- **Catchup:** `False`

