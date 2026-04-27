# NOVAX followup

[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afhængigheder) | [**Schedule**](#schedule)

## Formål

Formålet med followup-jobbet er at sikre, at patienter med kommende terminsdatoer (TERMIN) fortsat har korrekte adresse- og distriktsoplysninger i Novax. Jobbet kører periodisk og laver genopfølgning frem mod termin ved at genopslå CPR-adresse (inkl. beskyttet status), validere/normalisere adressen via Dataforsyningen og beregne distrikt via District Map.

Jobbet er designet som et supplement til det primære Novax-job: hvor hovedjobbet primært reagerer på nye/ændrede journaldata, er followup-jobbet terminsdrevet og genbesøger borgere med kommende terminsdatoer.

## Beskrivelse

Koden består af et DAG-job, der ved hvert run udfører følgende trin:

- Bestemmer "i dag" ved runtime (lokal dato på den worker, der afvikler tasken).
- Slår patienter op i Novax DB blandt patienter med tilknyttet `NameDetails` ved at filtrere på terminsdato (`NameDetails.TERMIN`) fra og med i dag (inkl.).
- For hver patient:
  - Validerer CPR-nummer.
  - Slår CPR op for at hente:
    - Adresse UUID (til Dataforsyningen)
    - “beskyttet adresse”-status
  - Opdaterer `NameDetails.BESKYTTETADRESSE` hvis CPR-status er ændret.
  - Slår adressen op i Dataforsyningen på CPR-adresse UUID.
    - Hvis Dataforsyningen returnerer uventet/ingen data for adressen, logges det, og patientens adresse/distrikt-opdatering springes over.
  - Hvis Dataforsyningen giver en gyldig adresse:
    - Opdaterer `Name.ADRESSE` hvis den fulde adresse er ændret.
    - Sikrer at adressens historik-tabeller holdes konsistente:
      - Eksisterende “åben” adresse-linje lukkes (slutdato sættes til nu), og der oprettes en ny adresse-linje med “åben” slutdato (Novax bruger typisk `1753-01-01` som sentinel for “open end”).
    - Slår distrikt op ud fra koordinater (x/y) via District Map.
    - Opdaterer `Name.DISTRIKT` hvis nyt distrikt kan bestemmes og afviger fra nuværende.
    - Opdaterer `NameDetails.TS_KOMID` til nyt distrikt, samt vedligeholder historik-tabellen for person-distrikter ved at lukke eksisterende “åben” række og oprette en ny.

**Vigtigt: “Always updates” pr. patient**

Uanset om der er detekteret ændringer i adresse/distrikt, udfører jobbet altid følgende opdatering pr. patient:

- Patienten sættes altid til aktiv (`AKTIV = 1`) hvis den er tom/0.

**Drift / sikkerhed**

- Jobbet kan køres i “dry-run” mode (styres af Airflow-variablen `NOVAX_DRY_RUN`, default `True`). Ved dry-run logges hvilke ændringer der ville blive skrevet, men der commits ikke til databasen.
- Opdateringer sker i én SQLAlchemy-session/transaction. Når `dry_run = False` commits ændringerne samlet til sidst.

**Dataflow:**

- Data fra Novax DB (TERMIN + eksisterende stamdata) → Adresseopslag via CPR → Adresser/validering via Dataforsyningen → Distriktsopslag via District Map → Opdatering i Novax DB

## Afhængigheder

:key: | **Airflow Connections**

**Novax DB:**
- **`novax_sql`**  
  Bruges som Connection id til Novax SQL-databasen (læse/skriv via SQLAlchemy engine).

**Dataforsyning API:**
- **`dataforsyningen`**  
  Bruges til at slå CPR-adresse UUID op i Dataforsyningen og hente felter som vejkode, kommunekode, postnr, koordinater m.m.

**District Map API/DB:**
- **`gis_db`**  
  Bruges til at oversætte koordinater (x/y) til distriktsnavn.

**CPR API:**
- **`cpr_replica_prod`**  
  Bruges til CPR-opslag (adresse UUID + beskyttet status).

## Schedule

Jobbet er sat op til at køre automatisk på følgende tidspunkter:

- **Tidspunkt:** Hver mandag kl. 01:15 CET/CEST (cron: `15 1 * * 1`, timezone: Europe/Copenhagen)
- **Catchup:** `False`
- **Concurrency:** `max_active_runs = 1`, retries sat til `0`

## Udvælgelse af patienter (TERMIN)

Jobbet behandler alle patienter med terminsdatoer fra og med dags dato (inkl.), dvs.:

- `NameDetails.TERMIN >= i dag kl. 00:00` (lokal tid på worker).

Der anvendes ikke længere planlagte dato-vinduer eller afgrænsning relativt til sidste DAG-run.
