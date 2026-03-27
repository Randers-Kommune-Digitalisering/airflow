# Nexus SMS Hjælpemiddelhuset Airflow DAG `README.md`
[**Formål**](#formål) | [**Dataflow**](#dataflow) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afhængigheder) | [**Schedule**](#schedule)

## Formål
Sende SMSer til modtagere af selvhentningsordre med en besked om at oderen kan hentes og hvad koden til døren er.

## Dataflow
Jobbet henter selvhentningsordre fra Nexus, sender SMS(er) og opdaterer ordre med en besked om status på at sende SMS.

## Afhængigheder

:key: | **Airflow Connections**

**Nexus:**
- **`nexus_prod`**
- **Bitwarden navn: `Nexus Randers Drift (client credentials)`**

**Conn Type**: HTTP

*Required felter*:
  - Connection id, Host, Login, Password, extra med token url

**Computronic:**
- **`computronic_89158600`**
- **Bitwarden navn: `Hjælpemiddelhuset SMS Login`**

**Conn Type**: HTTP

*Required felter*:
  - Connection id, Host, Login, Password

### Airflow Variables
**Dørkoder til hjælpemiddelshuset**
- **Key:** `hjaelpemiddelhuset_door_codes`
- **Format (JSON):**

## Schedule
Schedule er sat op til at køre automatisk på følgende tidspunkter:

- **Tidspunkt:** */5 * * * * (hvert 5. minut)
