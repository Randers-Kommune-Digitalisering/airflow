# Signflow SD Delta user creation `README.md`
[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afh%C3%A6ngigheder) | [**Schedule**](#schedule)

## Formål
Formålet med jobbet er at hente autorisationer fra Logiva Signflow, tjekke i Delta om der skal oprettes en bruger, hent data fra SD og indlæs i Delta.

## Beskrivelse
Henter autorisationer fra Logiva Signflow og tjekker, om der er en ansættelse i Delta. Hvis der er, og den ikke har en bruger, trækkes data fra SD for ansættelsen, og det tilføjes til en excelfil. Der sættes et 'x' i kolonnen "Handling" (hvilket trigger at Delta opretter en relation imellem en bruger og ansættelse), og filen indlæses i Delta, som derefter opretter en bruger.

Der genereres og uploades kun en fil ti lDelta hvis der er nogle ansættelser der mangler brugere.

### Logiva Signflow data
CSV-fil hentet fra Logiva Signflow er cp1252 encoded med tabs som seperator.
Filen indeholder kolonnerne: 'Navn', 'CPR', 'Tildelt Login', 'Loginnavn', 'Fra dato', 'LOS', 'Handling', 'Oprettelsestidspunkt', 'Sagsnummer', 'los1', 'los2', 'los3', 'los4', 'los5', 'los6', 'los7', 'los8', 'los9', 'lederemail'

### Delta excelfil 
Delta forventer en excelfil med ét ark der har kolonnerne: "Institutions-niveau", "Stamafdeling", "CPR-nummer", "Navn (for-/efternavn)", "Stillingskode nuværende", "Stillingskode niveau 2", "Startdato", "Slutdato", "Ansættelsesstatus", "Tjenestenummer", "Afdeling", "Handling"

**Dataflow:**
- Data fra Signflow → tjek Delta → hent data fra SD → lav excelfil → upload filen til Delta

## Afhængigheder

:key: | **Airflow Connections**

**SD API:**
- **`sd_silkeborgdata`**

**Conn Type**: HTTP

Auth til SD API

*Påkrævede felter*:
  - Connection ID, Host, Login, Password.

**Signflow API:**
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
  - Connection ID, Host, Login, Password og Extra (skal have token_url)

### Airflow Variables
**Ekko SD-afdelinger**
- **Key:** `delta_sd_insts_to_import`
- **Format (JSON):** `[
    {
        "inst_id": <id>,
        "excluded_dept_ids": [<id>, <id>]
    }, ...]`
- **Beskrivelse:** Liste af objekter med institutions-id og liste af afdelings-id'er for SD-afdelinger, der ikke skal medtages

## Schedule

Schedule er sat op til at køre automatisk på følgende tidspunkter:

- **Tidspunkt:** hver dag kl. 7:30
- **Cron syntax:**  
  ```
  30 7 * * *
  ```