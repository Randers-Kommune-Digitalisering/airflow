[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afhængigheder) | [**Schedule**](#schedule)

## Formål

Formålet med jobbet er at hente og opdatere patienters adresse- og distriktsinformationer samt evt. telefonnummer og terminsdato i Novax-systemet baseret på de nyeste journaldata og eksterne opslag. Dette sikrer, at distriktsoplysninger og kontaktdata altid er opdaterede i forhold til patientens aktuelle bopæl.

## Beskrivelse

Koden består af et DAG-job, der (for et automatisk beregnet datointerval) udfører følgende trin:

- Bestemmer datointerval automatisk baseret på sidste succesfulde scheduled DAG-run (start inklusiv, slut eksklusiv)
- Henter graviditetsjournaler fra Novax-databasen for intervallet (`get_pregnancy_journals`)
- Filtrerer dubletter pr. patient (NAVNID) og beholder kun seneste journalindslag i perioden
- For hver patient:
  - Springer over hvis der mangler journaltekst (NOTE)
  - Parser journaldata for at udtrække adresse, telefonnummer samt terminsdato
    - Hvis terminsdato ikke findes i journalen, beregnes den fra gestationsalder og journalens dato (`calculated_due_date`)
  - Henter aktuel adresse via CPR-opslag og forsøger at bruge CPR-adressen først
    - Hvis CPR-adresse ikke findes eller ikke kan parses, forsøges journal-adressen som fallback
    - Adressens “beskyttet”-status håndteres (CPR/Novax)
  - Validerer adressen i Dataforsyningen og henter nødvendige felter til videre opslag/opdatering
    - Koordinater (x/y) bruges til distriktsopslag
    - Vejkode bruges til historiktabellen for adresser i Novax
    - Kommunekode bruges til opdatering i Novax (ellers fallback til default)
    - Hvis adressen ikke kan findes i Dataforsyningen, opdateres adressen ikke (for at undgå at skrive en ukendt/ufuldstændig adresse i Novax)
  - Slår distriktsnavn op i GIS (District Map) på baggrund af koordinater (batch-opslag)
  - Udfører ekstra checks før opdatering i Novax:
    - Telefon opdateres kun hvis den er 8 cifre (ellers logges og ignoreres)
    - Distrikt opdateres kun hvis nyt distrikt kan bestemmes og det afviger fra nuværende
    - Kommune opdateres hvis den kan udledes fra Dataforsyningen; ellers sættes der en default kommune-kode når den nuværende ikke matcher default
  - Opdaterer Novax via en samlet batch (`update_novax_userdatas_batch`)

**Vigtigt: “Always updates” pr. patient**

Uanset om der er detekteret ændringer i adresse/distrikt/telefon/termin, udfører batch-opdateringen altid disse opdateringer pr. patient:

- Patienten tildeles altid til **“Gravid til fordeling”** ved at sætte `AnsvarsShpl = 'FIKTIV'` i Novax
- Patienten sættes altid til aktiv (`AKTIV = 1`) i Novax

**OBS:** Perioden for dataudtræk bestemmes automatisk ud fra sidste succesfulde scheduled run og det aktuelle runs data-interval (DAG'ens timezone). Intervaller behandles som hele dage, hvor start er inklusiv og slut er eksklusiv.

**Dataflow:**
- Data fra Novax DB (journal) → Adresseopslag via CPR og Dataforsyning → Distriktsopslag via District Map API → Opdatering i Novax DB

**Drift / sikkerhed**

- Jobbet kan køres i “dry-run” mode (styres af Airflow-variablen `NOVAX_DRY_RUN`), hvor der kun logges hvilke ændringer der ville blive skrevet, uden at opdatere Novax.
- Batch-opdateringen i Novax udføres i én session, men hver patient opdateres i en nested transaction (SAVEPOINT), så en fejl på én patient ikke nødvendigvis stopper hele batchen.

## Afhængigheder

:key: | **Airflow Connections**

**Novax DB:**
- **`novax_sql`**  
  Bruges som Connection id i Airflow til at hente host, database, bruger, adgangskode og port til Novax SQL-databasen.

**Dataforsyning API:**
- **`dataforsyningen`**  
  Bruges som Connection id i Airflow til at hente host til Dataforsyningens API.

**District Map API:**
- **`gis_db`**  
  Bruges som Connection id i Airflow til at hente host, bruger, adgangskode og port til GIS PostgreSQL databasen.

**CPR API:**
- **`cpr_replica_prod`**  
  Bruges som Connection id i Airflow til at hente host, client id, secret og token-url til CPR API'et.

## Schedule

Jobbet er sat op til at køre automatisk på følgende tidspunkter:

- **Tidspunkt:** Hver dag kl. 01:15 CET/CEST (00:15 UTC)
- **Cron syntax:**  
  ```
  15 1 * * *
  ```
