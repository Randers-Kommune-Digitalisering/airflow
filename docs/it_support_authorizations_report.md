# IT support authorizations report `README.md`
[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afh%C3%A6ngigheder) | [**Schedule**](#schedule)

## Formål

Formålet med jobbet er at sende en Excelrapport over relevante autorisationer fra Logiva Signflow med oplysninger om tilhørende ansættelser og loginnavne i Delta.

## Beskrivelse

Signflow leverer sagsnummer, navn, CPR, LOS, fra-dato, handling og ledermail. Autorisationer med handlingen `Nyansat` medtages op til 14 dage før fra-datoen; andre handlinger medtages fra og med fra-datoen. Rapporten sorteres efter fra-dato.

Delta slås op på CPR, LOS og fra-dato for aktive ansættelser. Rapporten indeholder kolonnerne `Sagsnummer`, `Navn`, `CPR`, `LOS`, `Loginnavn(e)`, `Fra dato`, `Handling`, `Lederemail`, `Findes ikke i Delta` og `Kommentar`. Manglende match markeres med `x`. Kommentaren angiver, om der er en aktiv ansættelse under et andet LOS, en ansættelse der ikke er aktiv på fra-datoen, eller ingen ansættelse fundet for CPR på fra-datoen. Den viser ikke CPR-værdien.

Excel-filen skrives til den midlertidige mappe `sd_delta_sync` og sendes som vedhæftning til modtagerne i Airflow Variable `it_support_authorizations_report_emails`. Rapporten indeholder personoplysninger; adgang til mail og midlertidige filer bør begrænses, og opbevaring/sletning bør afklares efter kommunens fælles processer.

**Dataflow:**
- Data fra Signflow → Delta-opslag → Excelrapport → e-mail

## Afhængigheder

:key: | **Airflow Connections**

**Logiva Signflow:**
- **`logiva_signflow`**

**Conn Type**: HTTP

Auth til Signflow API

*Påkrævede felter*:
  - Connection ID, Host, Login, Password.

**Delta API:**
- **`delta_prod`**

**Conn Type**: HTTP

Auth til Delta API

*Påkrævede felter*:
  - Connection ID, Host, Login, Password og Extra (skal have token_url).

Brug dedikerede legitimationsoplysninger til løsningen med mindst mulige rettigheder.

### Airflow Variables
**Rapportmodtagere**
- **Key:** `it_support_authorizations_report_emails`
- **Format (JSON):** `["modtager@example.org"]`
- **Beskrivelse:** Ikke-tom liste af mailadresser til rapporten. Listen føjes desuden til de almindelige fejlmailmodtagere ved opgavefejl. Variablen skal være oprettet, før DAG'en indlæses; en manglende eller ugyldig liste forhindrer indlæsning. Rapportmodtagere læses ved hver kørsel, mens ændringer i fejlmailmodtagere træder i kraft ved næste indlæsning af DAG'en.

## Schedule

Schedule er sat op til at køre automatisk på følgende tidspunkter:

- **Tidspunkt:** Hverdage kl. 6:30 (Europe/Copenhagen)
- **Cron syntax:**  
  ```
  30 6 * * 1-5
  ```
