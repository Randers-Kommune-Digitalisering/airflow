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
  - Springer over hvis patientens CPR er angivet i Airflow-variablen `NOVAX_IGNORE_CPRS` (kommasepareret liste).
  - Springer over hvis der ikke findes en matching graviditets-note (NOTE) for journalposten, inkl. check af tidspunkt (tillader typisk 0-1 minuts afvigelse).
  - Parser journaldata for at udtrække telefonnummer samt terminsdato
    - Hvis terminsdato ikke findes i journalen, beregnes den fra gestationsalder og journalens dato (`calculated_due_date`)
  - Henter aktuel adresse via CPR-opslag og forsøger at bruge CPR-adressen
    - Adressens “beskyttet”-status håndteres (CPR/Novax)
  - Validerer adressen i Dataforsyningen og henter nødvendige felter til videre opslag/opdatering
    - Koordinater (x/y) bruges til distriktsopslag
    - Vejkode/kommunekode/postnr m.m. bruges til at vedligeholde historiktabellen for adresser i Novax
    - Hvis adressen ikke kan findes i Dataforsyningen, logges det og patientens distrikt ryddes (se nedenfor)
  - Slår distriktsnavn op i GIS (District Map) på baggrund af koordinater
  - Udfører ekstra checks før opdatering i Novax:
    - Terminsdato opdateres kun hvis der findes en ny terminsdato i journalen (eller beregnet `calculated_due_date`) og den afviger fra eksisterende (`NameDetails.TERMIN`)
    - Telefon opdateres ud fra journalen ved at vedligeholde telefon-tabellen (`TELEFON`)
      - Primær telefon markeres via `Phone.PRIMAER` (eksisterende primær kan nedgraderes til `0`, og en eksisterende sekundær kan promoveres til `1`)
      - Hvis nummeret ikke findes, oprettes en ny række med `Phone.TELEFONNUMMER` og `Phone.PRIMAER = 1`
      - Ved opdatering/indsættelse opdateres relevante tidsstempler (`Phone.TS_DATE`, `Phone.TS_TIME`, `Phone.TS_UPDD`, `Phone.TS_UPDT`)
    - Beskyttet adresse-status synkroniseres fra CPR hvis den afviger (`NameDetails.BESKYTTETADRESSE`)
    - Adresse opdateres kun hvis Dataforsyningen giver en anden fuld adresse (`Name.ADRESSE`)
      - Samtidig vedligeholdes adressens historiktabel (`adrs`): åbne rækker lukkes via `Address.DATO_TIL`, og der indsættes evt. en ny række med bl.a. `Address.VEJKODE`, `Address.KOMMUNEKODE`, `Address.POSTNR`, `Address.STEDNAVN`, `Address.NR_LT_ETAGE`, `Address.DATO_FRA`, `Address.DATO_TIL`
      - Historikrækker timestamps vedligeholdes via `Address.TS_DATE`, `Address.TS_TIME`, `Address.TS_UPDD`, `Address.TS_UPDT`
    - Distrikt opdateres kun hvis nyt distrikt kan bestemmes og det afviger fra nuværende (`Name.DISTRIKT`)
      - Distriktsrelaterede felter opdateres (inkl. `Name.DISTRIKT` når distrikt ændres)
      - Historik for distrikter vedligeholdes i `PERSONDISTRICT`: åbne rækker lukkes via `PersonDistrict.DATETO`, og der indsættes evt. en ny række med `PersonDistrict.DISTRICT`, `PersonDistrict.DATEFROM`, `PersonDistrict.DATETO`
      - Historikrækker timestamps vedligeholdes via `PersonDistrict.TS_DATE`, `PersonDistrict.TS_TIME`, `PersonDistrict.TS_UPDD`, `PersonDistrict.TS_UPDT`
    - Hvis adressen ikke kan valideres/returneres fra Dataforsyningen, ryddes distrikt for patienten:
      - Åben række i `PERSONDISTRICT` lukkes (slutdato sættes til runtime)
      - `Name.DISTRIKT` sættes til tom streng
      - `NameDetails.TS_KOMID` sættes til tom streng
    - Kommune-ID opdateres i `Name.TS_KOMID` samt `NameDetails.TS_KOMID` og `NameDetails.KOMMUNE_OPR` hvis en valid adresse returneres fra Dataforsyningen
    - Tidsstempler opdateres ved ændringer (fx `Name.TS_UPDD`, `Name.TS_UPDT`, `NameDetails.TS_UPDD`, `NameDetails.TS_UPDT`)
  - Opdaterer Novax via en samlet batch

**Vigtigt: “Always updates” pr. patient**

Uanset om der er detekteret ændringer i adresse/distrikt/telefon/termin, forsøger jobkørslen altid at sikre disse værdier pr. patient (dvs. felterne skrives hvis de ikke allerede har den ønskede værdi):

- Patienten tildeles altid til **“Gravid til fordeling”** ved at sætte `AnsvarsShpl = 'FIKTIV'` i Novax
- Patienten sættes altid til aktiv (`AKTIV = 1`) i Novax

**OBS:** Perioden for dataudtræk bestemmes automatisk ud fra sidste succesfulde scheduled run og det aktuelle runs data-interval (DAG'ens timezone). Intervaller behandles som hele datoer, hvor start og slut anvendes direkte i databasefilteret (start `>=`, slut `<=`).

**Dataflow:**
- Data fra Novax DB (journal) → Adresseopslag via CPR og Dataforsyning → Distriktsopslag via District Map API → Opdatering i Novax DB

**Drift / sikkerhed**

- Jobbet kan køres i “dry-run” mode (styres af Airflow-variablen `NOVAX_DRY_RUN`), hvor der kun logges hvilke ændringer der ville blive skrevet, uden at opdatere Novax.
- Jobbet kan filtrere specifikke CPR-numre fra via Airflow-variablen `NOVAX_IGNORE_CPRS` (kommasepareret liste af CPR-numre).
- Dataforsyning-opslag har retry ved midlertidige fejl (timeouts og 5xx), og adressen behandles som “ikke fundet” hvis alle forsøg fejler.
- Batch-opdateringen i Novax udføres i én database-transaktion/session. Det betyder, at en fejl på én patient vil medføre, at hele transaktionen rulles tilbage, så enten bliver alle ændringer skrevet, eller også bliver ingen skrevet.
- Hvis der findes patienter med ugyldigt CPR-format (ikke 10 cifre), logges de og tasken fejler til sidst med en fejl (så det ikke bliver en “silent skip”).

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
