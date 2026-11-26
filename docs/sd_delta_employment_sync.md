# SD Delta employment sync `README.md`
[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afh%C3%A6ngigheder) | [**Schedule**](#schedule)

## Formål

Formålet med jobbet er at hente ændringer i SD, transformere dem og indlæse dem i Delta.

## Beskrivelse

For at indlæse ansættelser fra SD skal en excelfil indlæses i Delta, Delta forventer en fil med følgende headers:
"Institutions-niveau", "Stamafdeling", "CPR-nummer", "Navn (for-/efternavn)", "Stillingskode nuværende", "Stillingskode niveau 2", "Startdato", "Slutdato", "Ansættelsesstatus", "Tjenestenummer", "Afdeling", "Handling"

Ændringer i dataintervallet hentes fra SD, de nødvendige data hentes for ansættelserne fra SD, og en fil genereres. Perioder for samme ansættelse (institution, CPR-nummer og tjenestenummer) sorteres efter startdato i filen. Filen uploades, og en rapport sendes til delta@randers.dk.

Efter upload kontrollerer en Airflow-sensor importens XML-status hvert 30. sekund uden at holde en worker optaget mellem forespørgsler. Mailrapporten sendes efter statuskontrollen med samme indhold og vedhæftning som før. Importens resumé vises før proceslinket; resuméet vises med fed advarsel, hvis duplikerede, ugyldige eller afbrudte rækker er større end nul. Fejl og timeout efter 30 minutter vises samme sted uden at fejle jobbet. Uden en fil at uploade fortsætter jobbet uden statuskontrol. Rapport og vedhæftning overføres mellem opgaver via Airflow XCom.

**Dataflow:**
- Data fra SD → generér excelfil → upload filen til Delta → afvent importstatus → send mailrapport

## Afhængigheder

:key: | **Airflow Connections**

**SD API:**
- **`sd_silkeborgdata`**

**Conn Type**: HTTP

Auth til SD API

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

- **Tidspunkt:** 3 gange hver dag (kl. 7:00, 12:00 og 15:00)
- **Cron syntax:**  
  ```
  0 7,12,15 * * *
  ```