# SBSYS_LUK Airflow DAG README

[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afhængigheder) | [**Schedule**](#schedule)

## Formål

Formålet med jobbet er automatisk at lukke SBSYS-sager ud fra specifikke kriterier. I samme forbindelse journaliseres kladder på sagen (ved at oprette dem som dokumenter) og eventuelle erindringer afsluttes.

## Beskrivelse

DAG’en kører et Python-job, som finder relevante sager i SBSYS (via MSSQL/SQLAlchemy) og udfører følgende pr. sag:

1. Finder sager der matcher kriterierne:
	 - Sagsstatus (navn) er i en liste angivet via Airflow variabel.
	 - SagsskabelonID kan afgrænses til en “allow-list” (valgfrit) og/eller en “ignore-list”.
	 - Sagen har en sagspart (PartType = 1) hvor personen har civilstand **“Død”**.

2. Afslutter alle erindringer på sagen:
	 - Sætter `ErAfsluttet = 1`, udfylder afslutningsfelter og notat.

3. Journaliserer alle ikke-arkiverede kladder på sagen ved at oprette et dokument:
	 - Finder blob-indholdet i shardede `KladdeData` databaser.
	 - Opretter:
		 - `Dokument`
		 - `DokumentRegistrering` (kobler dokumentet på sagen)
		 - `DokumentDataInfo` (metadata om filen)
		 - Evt. `DelforloebDokumentRegistrering` links, hvis kladden var knyttet til delforløb.
	 - Kopierer blob-data fra `KladdeData` → `DokumentData` (indsættes i **nyeste** DokumentData-shard).
	 - Sætter kladden som arkiveret (`IsArchived = 1`).
	 - Sletter den oprindelige `KladdeData`-række for kladden.

4. Lukker sagen:
	 - Opdaterer `SagsStatusID` til “Lukket” og sætter status-change felter samt en standardkommentar.

### DRY_RUN / transaktion

- Jobbet kan køres i “dry-run” mode (Airflow variabel `SBSYS_LUK_DRY_RUN`), hvor der kun logges hvad der *ville* blive gjort, uden at skrive ændringer til databasen.
- Når dry-run er slået fra, udføres ændringerne i én session og commits til sidst. Fejl før commit betyder, at der ikke bliver skrevet delvise ændringer.

### Miljøvalg (Test/Drift)

Jobbet vælger miljø ud fra Airflow variabel `SBSYS_LUK_TEST_ENV`:

- `False` (default) ⇒ **Drift**
- `True` ⇒ **Test**

Miljøvalget påvirker:

- Hvilken Airflow connection der bruges (se “Afhængigheder”).
- Hvilken `SagsStatusID` der sættes ved lukning (production bruger ID 5, test bruger ID 8).
- Hvilke shard-databaser der søges i (navnemønstre inkluderer miljøet).

### Dataflow

- SBSYS “primær” DB (sager/kladder/registreringer) → opslag i shardede `KladdeData` DB’er → opret dokument + metadata i primær DB → indsæt blob i shardet `DokumentData` DB → arkiver kladde + slet `KladdeData` → luk sag.

## Afhængigheder

:key: | **Airflow Connections**

**SBSYS MSSQL:**

- **`sbsys_luk_Drift`**
	Bruges når `SBSYS_LUK_TEST_ENV = False`.

- **`sbsys_luk_Test`**
	Bruges når `SBSYS_LUK_TEST_ENV = True`.

Connection skal pege på en MSSQL-server, hvor brugeren har adgang til:

- Primære SBSYS-tabeller (sager, kladder, registreringer mv.).
- `sys.databases` (for at kunne finde shard databaserne via navnemønstre).
- Shard databaserne for `KladdeData` og `DokumentData` (læse/indsætte/slette).

:gear: | **Airflow variabler**

- `SBSYS_LUK_DRY_RUN`
	- `True` (default) logger handlinger uden at skrive.
	- `False` udfører opdateringer og commit.

- `SBSYS_LUK_TEST_ENV`
	- `True` bruger testmiljø.
	- `False` (default) bruger driftmiljø.

- `SBSYS_LUK_SAGSSTATUS`
	- Kommasepareret liste af sagsstatus-navne som må lukkes.
	- Default: `Aktiv`.

- `SBSYS_LUK_SAGSSKABELON_IDS`
	- Kommasepareret liste af SagsskabelonID’er (allow-list).
	- Tom streng betyder “ingen ekstra afgrænsning”.

- `SBSYS_LUK_SAGSSKABELON_IGNORE_IDS`
	- Kommasepareret liste af SagsskabelonID’er der altid ignoreres.
	- Tom streng betyder “intet ignoreres”.

## Schedule

Jobbet er sat op til at køre automatisk på følgende tidspunkter:

- **Tidspunkt:** `@weekly` (ugentligt)
- **Timezone:** Europe/Copenhagen
- **Startdato:** 2026-03-09
- **Catchup:** slået fra
