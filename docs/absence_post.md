# Fraværspost Airflow DAG `README.md`
[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afh%C3%A6ngigheder) | [**Schedule**](#schedule)

## Formål

Formålet med jobbet er at fordele fraværspost til de relevante afdelinger ud fra medarbejderens aktive SD-afdelingskode. Jobbet udtrækker CPR-nummeret fra PDF-filer modtaget i en postkasse, slår medarbejderens aktive ansættelser op i Delta og videresender dokumentet til de emailadresser, der er knyttet til afdelingen i den seneste SD-ORG-fil.

Løsningen består af to DAG'er: én til at synkronisere afdelingernes emailadresser fra SD-ORG og én til at behandle og videresende fraværspost.

## Beskrivelse

### Synkronisering af SD-ORG-afdelinger

DAG'en `absence_post_sync_sd_org_department_mapping` udfører følgende trin:

- Henter den nyeste ulæste Excel-vedhæftning fra fraværspostkassens `INBOX`, hvor filnavnet skal starte med `SD org`
- Finder kolonnen `NUV.` og mindst én af kolonnerne `Email 1`, `Email 2`, `Email 3` eller `Email 4`
- Opbygger en mapping fra SD-afdelingskode til en liste af unikke emailmodtagere
- Gemmer mappingen som JSON i Airflow Variablen `absence_post_mapning`
- Fejler, hvis der ikke findes en matchende Excel-fil, de nødvendige kolonner mangler, eller filen ikke indeholder gyldige mappings

### Behandling og videresendelse af fraværspost

DAG'en `absence_post_forward_mails` udfører følgende trin:

- Henter alle emails fra fraværspostkassens `INBOX`
- Behandler kun emails, hvis emnet indeholder et fragment fra `allowed_subject_fragments`. Andre emails bliver liggende urørt. 
- Behandler vedhæftninger, hvor filnavnet starter med `maindoc` og slutter med `.pdf`
- Udtrækker CPR-nummer fra teksten i hver PDF
- Slår aktive ansættelser og SD-afdelingskoder op i Delta pr. CPR
- Genbruger Delta-opslaget, hvis det samme CPR optræder i flere vedhæftninger
- Finder afdelingens modtagere i Airflow Variablen `absence_post_mapning`
- Videresender PDF'en med emailens oprindelige emne og en konfigureret brødtekst alt efter emnet på mailen
- Indleder den videresendte mail med `default_welcome_body`, når værdien er konfigureret
- Tilføjer altid `default_closing_body`, når værdien er konfigureret
- Erstatter den oprindelige brødtekst, hvis emnet indeholder en nøgle fra `subject_body_mapping` første match anvendes
- Sletter den oprindelige email efter en vellykket videresendelse

Hvis der ikke findes præcis én anvendelig SD-afdelingskode, videresendes dokumentet ikke automatisk:

- Ved ingen aktiv eller mappet afdeling springes vedhæftningen over
- Ved flere aktive afdelinger samles personens navn, afdelingskoder og PDF-filer i en opsummeringsmail til den ansvarlige modtager. Her skal den ansvarlige manuelt videresende mailen til den korrekte person


**Dataflow:**
- SD-ORG-email (IMAP) + SD ORG Excel-vedhæftning → afdelings- og emailmapping → Airflow Variable
- Fraværspost-email (IMAP) + PDF-vedhæftning → CPR-nummer → Delta-opslag → afdelingsmodtagere → videresendt email (SMTP)

**Bemærk (datahåndtering):**

- Den oprindelige email slettes først efter vellykket videresendelse. Emails med dokumenter, der springes over, forbliver i postkassen.
- Opsummeringsmailen ved flere aktive afdelinger indeholder personnavne og de oprindelige PDF-filer. Her skal den ansvarlige tage stilling til hvad man gør med det

**Forudsætning (manuel proces):**

Personale og HR sender en opdateret SD-ORG Excel-fil til fraværspostkassen. Filnavnet skal starte med `SD org`, og filen skal indeholde kolonnen `NUV.` samt mindst én af emailkolonnerne `Email 1` til `Email 4`.

DAG'en `absence_post_sync_sd_org_department_mapping` skal køres manuelt, når afdelingernes emailmapping skal opdateres.

## Afhængigheder

### Delta

Delta anvendes til at finde personens aktive ansættelser, navn og SD-afdelingskode på behandlingsdatoen.

### Airflow Connections
:key: | **Airflow Connections**

**IMAP (fraværspostkasse):**
- **`absence_post_imap`**
- **Bitwarden navn: `Postkasse - FravaerPost`**

Bruges til at hente login og password til den postkasse, som DAG'en læser input fra.

*Required felter*:
- Connection id, Username (Login) og Password

**Delta API:**
- **`delta_prod`**
- **Bitwarden navn: `Delta prod (ny)`**

Bruges til opslag af aktive ansættelser, SD-afdelingskoder & fulde navn 

*Required felter*:
- Connection id, Host, Username (Client ID) og Password (Client Secret)
- Extra-feltet `token_url`

### Airflow Variables
:key: | **Airflow Variables**

**Fraværspost runtime-konfiguration:**
- **Key**: `absence_post_config`


*Required felter*:
- `sender_email`
- `smtp_server`
- `imap_server`
- `allowed_subject_fragments` (ikke-tom liste af emnefragmenter; ekstra tekst i emnet er tilladt)

*Valgfrie felter*:
- `subject_body_mapping`
- `default_welcome_body` (tekst før brødteksten, fx `Kære leder,`)
- `default_closing_body`

Eksempel:
```json
{
	"sender_email": "no-reply@randers.dk",
	"smtp_server": "smtp.example.local",
	"imap_server": "imap.example.local",
	"allowed_subject_fragments": ["Vi mangler oplysninger fra jeres medarbejder"],
	"default_welcome_body": "Kære leder,",
	"subject_body_mapping": {
		"sygefravær": "Konfigureret tekst til emails om sygefravær"
	},
	"default_closing_body": "OBS: Dette er en automatisk løsning. Hvis du har modtaget en mail ved en fejl så kontakt denne mail: xx.xx@randers.dk"
}
```

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
