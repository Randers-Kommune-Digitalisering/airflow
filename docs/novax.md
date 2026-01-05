[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afhængigheder) | [**Schedule**](#schedule)

## Formål

Formålet med jobbet er at hente og opdatere patienters adresse- og distriktsinformationer samt evt. telefonnummer og terminsdato i Novax-systemet baseret på de nyeste journaldata og eksterne opslag. Dette sikrer, at distriktsoplysninger og kontaktdata altid er opdaterede i forhold til patientens aktuelle bopæl.

## Beskrivelse

Koden består af et DAG-job, der udfører følgende trin:

- Henter graviditetsjournaler fra Novax-databasen for en given periode (`get_pregnancy_journals`)
- For hver patient:
  - Parser journaldata for at udtrække ny adresse samt evt. telefonnummer og terminsdato
  - Hvis terminsdato mangler i journalen, beregnes denne fra gestationsalder (i journalen)
  - Trækker aktuelle adresse fra CPR-opslag via API
  - Validerer og finder distriktsoplysninger via Dataforsyning API'et og District Map API'et
  - Sammenligner ny og eksisterende adresse/distrikt og opdaterer Novax-databasen med ny data (`update_novax_userdata`)

**OBS:** Perioden for dataudtræk bestemmes automatisk ud fra, hvornår jobbet sidst blev kørt. Det betyder, at jobbet som standard behandler alle nye eller ændrede data frem til og med i går, medmindre du selv vælger en anden periode.

**Dataflow:**
- Data fra Novax DB (journal) → Adresseopslag via CPR og Dataforsyning → Distriktsopslag via District Map API → Opdatering i Novax DB

## Afhængigheder

:key: | **Airflow Connections**

**Novax DB:**
- **`novax_sql_default`**  
  Bruges som Connection id i Airflow til at hente host, database, bruger, adgangskode og port til Novax SQL-databasen

**Dataforsyning API:**
- **`dataforsyning_default`**  
  Bruges som Connection id i Airflow til at hente host og evt. nøgle til Dataforsyning API'et

**District Map API:**
- **`district_map_default`**  
  Bruges som Connection id i Airflow til at hente host til District Map API'et

**CPR API:**
- **`cpr_replica_prod`**  
  Bruges som Connection id i Airflow til at hente host, client id, secret og token-url til CPR API'et

## Schedule

Jobbet er sat op til at køre automatisk på følgende tidspunkter:

- **Tidspunkt:** Kl. 01:00 hver nat
- **Cron syntax:**  
  ```
  0 1 * * *
  ```
