# Novax Airflow DAG README

[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afhængigheder) | [**Schedule**](#schedule)

## Formål

Formålet med jobbet er at hente og opdatere patienters adresse- og distriktsinformationer samt evt. telefonnummer og terminsdato i Novax-systemet baseret på de nyeste journaldata og eksterne opslag. Dette sikrer, at distriktsoplysninger og kontaktdata altid er opdaterede i forhold til patientens aktuelle bopæl.

## Beskrivelse

Koden består af et DAG-job, der (for et automatisk beregnet datointerval) udfører følgende trin:

- Bestemmer datointerval automatisk baseret på sidste succesfulde scheduled DAG-run (kun `run_type = scheduled` og `state = success`). Intervallet beregnes fra sidste runs `data_interval_end` frem til det aktuelle runs `data_interval_end`.
- Henter graviditetsjournaler fra Novax-databasen for intervallet ved at udvælge journalposter med emne som “Orientering - Gravid”.
- Filtrerer dubletter pr. patient (NAVNID) og beholder kun seneste journalindslag i perioden.
- For hver patient:
  - Springer over hvis der ikke findes en matching graviditets-note (NOTE) for journalposten, inkl. check af tidspunkt (tillader typisk 0-1 minuts afvigelse).
  - Parser journaldata for at udtrække telefonnummer samt terminsdato
    - Hvis terminsdato ikke findes i journalen, beregnes den fra gestationsalder og journalens dato (`calculated_due_date`)
  - Henter aktuel adresse via CPR-opslag og forsøger at bruge CPR-adressen
    - Adressens “beskyttet”-status håndteres (CPR/Novax)
  - Validerer adressen i Dataforsyningen og henter nødvendige felter til videre opslag/opdatering
    - Koordinater (x/y) bruges til distriktsopslag
    - Vejkode/kommunekode/postnr m.m. bruges til at vedligeholde historiktabellen for adresser i Novax
    - Hvis adressen ikke kan findes i Dataforsyningen, logges det og patientens adresse/distrikt-opdatering springes over
  - Slår distriktsnavn op i GIS (District Map) på baggrund af koordinater
  - Udfører ekstra checks før opdatering i Novax:
    - Telefon opdateres
    - Distrikt opdateres kun hvis nyt distrikt kan bestemmes og det afviger fra nuværende
    - Distriktsrelaterede felter opdateres (inkl. `NameDetails.TS_KOMID` når distrikt ændres)
  - Opdaterer Novax via en samlet batch

**Vigtigt: “Always updates” pr. patient**

Uanset om der er detekteret ændringer i adresse/distrikt/telefon/termin, udfører batch-opdateringen altid disse opdateringer pr. patient:

- Patienten tildeles altid til **“Gravid til fordeling”** ved at sætte `AnsvarsShpl = 'FIKTIV'` i Novax
- Patienten sættes altid til aktiv (`AKTIV = 1`) i Novax

**OBS:** Perioden for dataudtræk bestemmes automatisk ud fra sidste succesfulde scheduled run og det aktuelle runs data-interval (DAG'ens timezone). Intervaller behandles som hele datoer, hvor start og slut anvendes direkte i databasefilteret (start `>=`, slut `<=`).

**Dataflow:**
- Data fra Novax DB (journal) → Adresseopslag via CPR og Dataforsyning → Distriktsopslag via District Map API → Opdatering i Novax DB

**Drift / sikkerhed**

- Jobbet kan køres i “dry-run” mode (styres af Airflow-variablen `NOVAX_DRY_RUN`), hvor der kun logges hvilke ændringer der ville blive skrevet, uden at opdatere Novax.
- Batch-opdateringen i Novax udføres i én database-transaktion/session. Det betyder, at en fejl på én patient vil medføre, at hele transaktionen rulles tilbage, så enten bliver alle ændringer skrevet, eller også bliver ingen skrevet.

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

- **Tidspunkt:** Hver dag kl. 00:00 CET/CEST
