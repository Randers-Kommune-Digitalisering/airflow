# Frontdesk Airflow DAG `README.md`
[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) |
[**Afhængigheder**](#afh%C3%A6ngigheder) | [**Airflow Variable**](#airflow-variable) | [**Schedule**](#schedule) 

## Formål

Formålet med jobbet er at hente rå Frontdesk-operationer fra MSSQL, transformere data til rapportering og prognoser og gemme resultatet i Postgres.

## Beskrivelse

**Jobbet ufører følgende trin:**

- Opretter forbindelse til Frontdesk-kildedatabasen
- Henter rå operationer fra Operation-tabellen
- Validerer at nødvendige kolonner findes
- Normaliserer dato- og tidsfelter samt fjerner irrelevante kolonner
- Filtrerer rækker:
    - Ekskluderer tællere, der er uden for Borgerservise-scope
    - beholder kun nyere daya (seneste 2 år)
- Giver data felter
    - Dato, uge og år
    - Queue-gruppering
    - Ventetid og behandlingstid
- Gemmer transformerede operationer i Postgres-tabellen operations
- Beregner forecast med Prophet
    - Samlet forecast
    - Kun hverdage medtages
- Gemmer forecast i Postgres-tabellen forecasts

Hvis der ikke er data efter filtrering, eller forecast-output er ugyldigt, stopper jobbet bevidst med fejl.

**Dataflow:**
- Frontdesk MSSQL -> transformering og forecast -> Postgres (operations og forecasts)

## Afhængigheder
:key: | **Airflow Connections**
**Frontdesk-kilde (MSSQL)**
- **`azure_frontdesk_db`**

**Conn type**: MYSSQL

bruges som `Connection id`i Airflow til at hente host, database, user, pass og port til Frontdesk-kilden

*Required felter*:
- Connection id, Host, DataBase, Login, Password og Port

**Frontdesk-mål (Postgres)**:
- **`frontdesk_db`**

**Conn type**: Postgres

Bruges som `Connection id` i Airflow til at gemme operations- og forecast-data i Postgres

*Required felter*:
- Connection id, Host, Database, User, Pass og Port'

## Airflow Variable

Opret eller opdater Airflow Variable `frontdesk_runtime_config` med JSON. Behold de queue-værdier, der allerede findes i `queues`; feltet bestemmer, hvilke grupper der får deres eget forecast. `samlet` forecast beregnes separat.

```json
{
    "queues": [
      "Pas", 
      "MitID",
      "Afhent pas/kørekort/sundhedskort",
      "Kørekort",
      "Pension",
     "Informationen",
     "Buskort til pensionister",
     "Andet",
     "Beboerindskud og boligstøtte",
     "Skat",
     "Flytning og Folkeregister",
     "Sundhedskort og lægevalg",
     "Legitimationskort",
     "Tilflytning fra udlandet",
     "Fritagelse for digitalpost"
    ],
    "queue_groups": {
        "Afhent pas/kørekort/sundhedskort ": "Afhent pas/kørekort/sundhedskort",
        "Beboerindskud ": "Beboerindskud og boligstøtte",
        "Beboerindskud og boligstøtte ": "Beboerindskud og boligstøtte",
        "Boligstøtte": "Beboerindskud og boligstøtte",
        "Den Boligsociale Enhed - Anders": "Den Boligsociale Enhed",
        "Den Boligsociale Enhed  - Janni": "Den Boligsociale Enhed",
        "Flytning og folkeregister ": "Flytning og Folkeregister",
        "Flytning og Folkeregister": "Flytning og Folkeregister",
        "Kontrolenheden - Bjørn": "Kontrolenheden",
        "Kontrolenheden - Erik": "Kontrolenheden",
        "Kontrolenheden - Marianne": "Kontrolenheden",
        "Kørekort ": "Kørekort",
        "Kørekort Selfie": "Kørekort",
        "Legitimationskort/ID-kort": "Legitimationskort",
        "Legitimationskort ": "Legitimationskort",
        "MitID ": "MitID",
        "MitID": "MitID",
        "Mit-ID Aktiveringskode": "MitID",
        "Pas ": "Pas",
        "Pas Selfie": "Pas",
        "Pension ": "Pension",
        "Pension": "Pension",
        "Pension akut tid": "Pension",
        "Resultat af årsopgørelse": "Skat",
        "SKAT": "Skat",
        "Skat": "Skat"
    },
    "dropped_columns": [
        "MunicipalityID",
        "QueueId",
        "QueueCategoryId",
        "State",
        "StateId",
        "CounterId",
        "EmployeeId",
        "DelayedUntil",
        "DelayedFrom",
        "IsEmployeeAnonymized",
        "EmployeeInitials"
    ],
    "excluded_counters": [
        "Jobcenter",
        "Ydelseskontoret",
        "Integration"
    ],
    "datetime_columns": [
        "CreatedAt",
        "CalledAt",
        "EndedAt",
        "LastAggregatedDataUpdateTime"
    ],
    "operation_column_renames": {
         "Id": "id",
         "MunicipalityID": "municipality_id",
         "QueueId": "queue_id",
         "QueueName": "queue_name",
         "QueueCategoryId": "queue_category_id",
         "CategoryName": "category_name",
         "StateId": "state_id",
         "State": "state",
         "TicketId": "ticket_id",
         "CounterId": "counter_id",
         "CounterName": "counter_name",
         "CalledAt": "called_at",
         "CreatedAt": "created_at",
         "EndedAt": "ended_at",
         "EmployeeId": "employee_id",
         "EmployeeName": "employee_name",
         "DelayedUntil": "delayed_until",
         "DelayedFrom": "delayed_from",
         "IsEmployeeAnonymized": "is_employee_anonymized",
         "AggregatedWaitingTime": "aggregated_waiting_time",
         "LastAggregatedDataUpdateTime": "last_aggregated_data_update_time",
         "EmployeeInitials": "employee_initials",
         "AggregatedProcessingTime": "aggregated_processing_time",
         "QueuesGrouped": "queues_grouped",
         "BehandlingstidMinutter": "behandlingstid_minutter",
         "BehandlingstidMinutterDecimal": "behandlingstid_minutter_decimal",
         "VentetidMinutter": "ventetid_minutter",
         "VentetidMinutterDecimal": "ventetid_minutter_decimal"
    }
}   
```

`queue_groups` samler rå queue-navne under rapporteringsnavne. De øvrige lister bestemmer kolonner, der fjernes, tællere, der udelukkes, og kolonner, der parses som dato/tid.

Opdater Airflow Variable før den nye kode køres; DAG'en stopper med en tydelig fejl, hvis et påkrævet felt mangler eller har forkert format.

## Schedule

Schedule er sat op til at køre automatisk på følgende tidspunkter:

- **Tidspunkt:** hver uge 
