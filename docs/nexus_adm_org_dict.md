# Nexus Adm. Org. dict Airflow DAG `README.md`
[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afh%C3%A6ngigheder) | [**Schedule**](#schedule)

## Formål

Formålet med jobbet er at hente UUID'er for administrative enheder, der er relevante for Nexus-brugere fra Delta og sætte resultatet som en Airflow variabel.

## Beskrivelse

Koden består af et DAG-job, der udfører følgende trin:

- Henter alle Adm. org. under top UUID fra Delta
- Trækker kun uuid'er og fjerner adm. org. uden medarbejdere.
- Et dict bygges hvor uuid sættes som key og en liste af uuids som value (under adm. org.)
- Resultatet gemmes som json i variablen 'nexus_adm_org_dict'

**Dataflow:**
- Data fra Delta → Data behandles → Data gemmes i en Airflow variabel.

## Afhængigheder

:key: | **Airflow Connections + Variables**

**Delta API:**
- **`delta_prod`**

**Conn Type**: HTTP

Auth til Delta API

*Påkrævede felter*:
  - Connection id, Host, Login, Password og Extra (skal have token_url)

**Variable:**
- **`nexus_top_adm_org_uuid`**

**Type**: variable

 Bruges som øverste adm. org. uuid hvor all under hentes

 *Påkrævede felter*:
  - Val

## Schedule

Schedule er sat op til at køre automatisk på følgende tidspunkter:

- **Tidspunkt:** Kl. 00:00 hver dag
