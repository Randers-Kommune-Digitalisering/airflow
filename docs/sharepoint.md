# SharePoint Airflow DAG `README.md`
[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afh%C3%A6ngigheder) | [**Schedule**](#schedule)

## Formål

Formålet med dette job er at hente data fra en SharePoint-liste via Microsoft Graph API og gemme det i en PostgreSQL-database.

## Beskrivelse

Koden består af et Airflow DAG-job, der udfører følgende trin:

- Henter SharePoint-listeelementer via Microsoft Graph API (`process_sharepoint_list_items`)
- Transformerer og renamer felter, så de matcher ønsket format
- Gemmer de transformerede data i en Postgres-database

**Dataflow:**
- Data fra SharePoint → Data transformeres → Data gemmes i Postgres DB

## Afhængigheder

:key: | **Airflow Connections**

**SharePoint/Graph API:**
- **`ms_graph_sharepoint_handleplan`**  
  Bruges som `Connection id` i Airflow til at hente adgang til Microsoft Graph API.

**SharePoint Config:**
- **`sharepoint_handleplan_config`**  
  Bruges som `Connection id` i Airflow til at hente site- og list-id fra `extra`-feltet.

**Postgres DB:**
- **`sharepoint_db`**  
  Bruges som `Connection id` i Airflow til at hente host, database, user, pass og port til SharePoint Postgres DB'en.

## Schedule

DAG'et er sat op til at køre automatisk på følgende tidspunkter:

- **Tidspunkt:** Kl. 00:00 hver eneste døgn
- **Cron syntax:**  
  ```
  0 0 * * *
  ```