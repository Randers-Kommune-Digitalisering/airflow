# Fraværspost Airflow DAG `README.md`
[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afh%C3%A6ngigheder) | [**Schedule**](#schedule)

## Formål

Formålet med jobbet er at fordele fraværspost til de relevante afdelinger ud fra medarbejderens aktive SD-afdelingskode. Jobbet udtrækker CPR-nummeret fra PDF-filer modtaget i Fraværs postkassen, slår medarbejderens aktive ansættelser op i Delta, journaliserer dokumentet i SBSYS på et specifikt Delforløb og orienterer de emailadresser, der er knyttet til afdelingen i den seneste SD-ORG-fil. 

Løsningen består af to DAG'er: én til at synkronisere afdelingernes emailadresser fra SD-ORG og én til at behandle og videresende fraværspost.

## Beskrivelse

### Synkronisering af SD-ORG-afdelinger

DAG'en `absence_post_sync_sd_org_department_mapping` udfører følgende trin:

- Henter den nyeste ulæste Excel-vedhæftning fra SD-ORG-mapping-postkassens `INBOX` via `sd_org_mapping_imap`, hvor filnavnet skal starte med `SD org`
- Finder kolonnen `NUV.` og mindst én af kolonnerne `Email 1`, `Email 2`, `Email 3` eller `Email 4`
- Opbygger en mapping fra SD-afdelingskode til en liste af unikke emailmodtagere
- Gemmer mappingen som JSON i Airflow Variablen `absence_post_mapning`
- Fejler, hvis der ikke findes en matchende Excel-fil, de nødvendige kolonner mangler, eller filen ikke indeholder gyldige mappings

### Behandling og videresendelse af fraværspost

DAG'en `absence_post_forward_mails` udfører følgende trin:

- Henter alle emails fra fraværspostkassens `INBOX` via `fravaer_post_imap`
- Behandler kun emails, hvis emnet indeholder et fragment fra `allowed_subject_fragments` fra Airflow Variablen `absence_post_config`. Andre emails bliver liggende urørt.
	- F.eks: `Vi mangler oplysninger fra jeres medarbejder`
- Behandler vedhæftninger, hvor filnavnet starter med `maindoc` og slutter med `.pdf`
- Udtrækker CPR-nummer fra teksten i hver PDF
- Slår aktive ansættelser og SD-afdelingskoder op i Delta pr. CPR
- Genbruger Delta-opslaget, hvis det samme CPR optræder i flere vedhæftninger
- Finder afdelingens modtagere i Airflow Variablen `absence_post_mapning`
- Slår personalesager op i SBSYS Produktion via CPR og journaliserer PDF'en på hver aktiv sag (status-id 9). Delforløbet vælges ud fra mailens emne via `subject_delforloeb_mapping`: kun Delforløbet `3.1 Breve fra Udbetaling Danmark` og `02 Sygdom` er tilladt at blive journaliseret på. Mailen bliver liggende ved manglende eller modstridende emnematch. Hvis delforløbet mangler på en aktiv sag, oprettes det før journalisering
- Sender derefter en mail til afdelingens modtagere med det oprindelige emne, den konfigurerede brødtekst samt delforløbets titel og sagsnumrene for de journaliserede sager
- Sletter kun den oprindelige email, når alle relevante PDF'er er journaliseret og deres modtagere er orienteret. Ved fejl bliver mailen liggende til manuel håndtering

Hvis der ikke findes præcis én anvendelig SD-afdelingskode, videresendes dokumentet ikke automatisk:

- Ved ingen aktiv eller mappet afdeling springes vedhæftningen over
- Ved flere aktive afdelinger samles personens navn, afdelingskoder og PDF-filer i en opsummeringsmail til de konfigurerede ansvarlige modtagere. Hvis modtagerlisten ikke er konfigureret, springes opsummeringsmailen over, og de oprindelige mails bliver liggende til manuel håndtering


**Dataflow:**
- SD-ORG-email (IMAP) + SD ORG Excel-vedhæftning → afdelings- og emailmapping → Airflow Variable
- Fraværspost-email (IMAP) + PDF-vedhæftning → CPR-nummer → Delta-opslag → journalisering på aktive personalesager i SBSYS → mail til afdelingsmodtagere (SMTP) → Slet original mail fra postkassen

**Bemærk (datahåndtering):**

- Den oprindelige email slettes først efter vellykket journalisering og afsendelse af orienteringsmailen. Emails med dokumenter, der springes over, forbliver i postkassen. 
- Opsummeringsmailen ved flere aktive afdelinger indeholder personnavne og de oprindelige PDF-filer. Her skal den ansvarlige tage stilling til hvad man gør med det

**Forudsætning (manuel proces):**

Personale og HR sender en opdateret SD-ORG Excel-fil til SD-ORG-mapping-postkassen (`sd_org_mapping_imap`). Filnavnet skal starte med `SD org`, og filen skal indeholde kolonnen `NUV.` samt mindst én af emailkolonnerne `Email 1` til `Email 4`.

DAG'en `absence_post_sync_sd_org_department_mapping` skal køres manuelt, når afdelingernes emailmapping skal opdateres.

## Afhængigheder

### Delta

Delta anvendes til at finde personens aktive ansættelser, navn og SD-afdelingskode på behandlingsdatoen.

### SBSYS Produktion

For hver PDF slås personens personalesager op via Airflow Connection `sbsys_api_prod`. Journalisering kræver mindst én aktiv personalesag (status-id 9); ved manglende aktiv sag beholdes mailen i postkassen, og der sendes ingen orienteringsmail. Det valgte delforløb oprettes på hver aktiv sag, hvis det mangler. Der findes et udkommenteret emnesag-testflow med `sbsys_api_test` til lokal test

### Airflow Connections
:key: | **Airflow Connections**


De to DAG'er bruger hver sin IMAP-connection. Til lokal test kan fraværspost-taskens connection midlertidigt ændres til `sd_org_mapping_imap` i koden; den aktive kode bruger `fravaer_post_imap`.

**IMAP (SD-ORG-mapping):**
- **`sd_org_mapping_imap`**

Bruges til at hente den nyeste ulæste SD-ORG Excel-vedhæftning til DAG'en `absence_post_sync_sd_org_department_mapping`.

*Required felter*:
- Connection id, Username (Login) og Password

**IMAP (fraværspost):**
- **`fravaer_post_imap`**

Bruges til at hente fraværsmails til DAG'en `absence_post_forward_mails`.

*Required felter*:
- Connection id, Username (Login) og Password

**Delta API:**
- **`delta_prod`**
- **Bitwarden navn: `Delta prod (ny)`**

Bruges til opslag af aktive ansættelser, SD-afdelingskoder & fulde navn 

*Required felter*:
- Connection id, Host, Username (Client ID) og Password (Client Secret)
- Extra-feltet `token_url`

**SBSYS Produktion:**
- **`sbsys_api_prod`**

Bruges til at slå personalesager op, hente/oprette delforløb og journalisere PDF'er.

*Required felter*:
- Connection id, Host (SBSYS API-base url), Username (Client ID) og Password (Client Secret)
- Extra-felterne `token_url`, `username` og `password` til SBSYS-login

**SBSYS Test:**
- **`sbsys_api_test`**

Bruges til at test Emnesag flowet med lokalt

*Required felter*:
- Connection id, Host (SBSYS API-base url), Username (Client ID) og Password (Client Secret)
- Extra-felterne `token_url`, `username` og `password` til SBSYS-login

### Airflow Variables
:key: | **Airflow Variables**

**Fraværspost runtime-konfiguration:**
- **Key**: `absence_post_config`


*Required felter*:
- `sender_email`
- `smtp_server`
- `imap_server`
- `allowed_subject_fragments` (ikke-tom liste af emnefragmenter; ekstra tekst i emnet er tilladt)
- `subject_delforloeb_mapping` (JSON-objekt med emnefragmenter som nøgler og `3.1 Breve fra Udbetaling Danmark` eller `02 Sygdom` som værdier)

*Valgfrie felter*:
- `subject_body_mapping`
- `default_welcome_body` (tekst før brødteksten, fx `Kære leder,`)
- `default_closing_body`
- `multi_department_notification_recipients` liste af emailadresser, der modtager opsummeringsmailen med PDF-filer ved flere aktive SD-afdelingskoder

Eksempel:
```json
{
	"sender_email": "no-reply@randers.dk",
	"smtp_server": "smtp.example.local",
	"imap_server": "imap.example.local",
	"allowed_subject_fragments": ["Vi mangler oplysninger fra jeres medarbejder", "Ændring af sidste orlovsdag"],
	"subject_delforloeb_mapping": {
		"Vi mangler oplysninger fra jeres medarbejder": "3.1 Breve fra Udbetaling Danmark",
		"Ændring af sidste orlovsdag": "02 Sygdom"
	},
	"multi_department_notification_recipients": ["test@randers.dk"],
	"default_welcome_body": "Kære leder,",
	"subject_body_mapping": {
		"sygefravær": "Konfigureret tekst til emails om sygefravær"
	},
	"default_closing_body": "Med venlig hilsen"
}
```

Tilføj nye emnefragmenter i både `allowed_subject_fragments` og `subject_delforloeb_mapping`. Match er uafhængigt af store/små bogstaver og accepterer ekstra tekst i emnet. `subject_body_mapping` styrer kun teksten i orienteringsmailen. Ukendte eller tvetydige emner bliver liggende uden journalisering; en fejl under oprettelse af delforløb eller journalisering forhindrer orienteringsmailen, og den oprindelige mail beholdes.

**Mapping mellem SD-afdelinger og emailmodtagere:**
- **Key**: `absence_post_mapning`

Variablen oprettes og opdateres automatisk af DAG'en `absence_post_sync_sd_org_department_mapping`. Den skal være et JSON-objekt, hvor hver afdelingskode indeholder en liste af emailmodtagere.

Eksempel:
```json
{
	"ABCD": [
		"modtager1@randers.dk"
	],
	"BOOL": [
		"modtager1@randers.dk",
		"modtager2@randers.dk"
	]
}
```

## Schedule

### Synkronisering af SD-ORG-mapping


Personale og HR har adgang til UI'en i Airflow med rollen: `Absence`. Her kan de selv trigger DAG'en `absence_post_sync_sd_org_department_mapping` efter eget behov.


### Videresendelse af fraværspost

DAG'en `absence_post_forward_mails` kører dagligt kl 09:00 (`0 9 * * *`)
