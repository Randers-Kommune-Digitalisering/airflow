# Vognpark Hent Insubiz data til database
[Formål](#formål) | [Beskrivelse](#beskrivelse) | [Output](#output) | [Afhængigheder](#afhængigheder) | [Schedule](#schedule)

## Formål

Formålet med denne DAG er at hente nyeste køretøjsdata fra Insubiz og gemme datasættet i Postgres DB

## Beskrivelse

**DAG:** `dag_vognpark_insubiz_data_to_db`

Udfører følgende:

- Henter alle køretøjer fra Insubiz API.
- Henter alle kunder fra Insubiz API og beriger køretøjer med `Level1` til `Level6`.
- Normaliserer og formatterer datasættet.
- Beregner `report_date` ud fra DAG'ens logical date i tidszonen `Europe/Copenhagen`.
- Gemmer data i PostgreSQL-tabellen `vognpark_data`.
- Gemmer seneste kørselsdato i PostgreSQL-tabellen `vognpark_run_audit`.


## Output

Flowet opdaterer data i PostgreSQL:

- Tabellen `vognpark_data` erstattes med nyeste udtræk fra Insubiz
- Tabellen `vognpark_run_audit` erstattes med seneste `report_date`.

## Afhængigheder

### Airflow Connections

**Postgres DB**

- Connection id: `vognpark_db`
- Type: Postgres
- Krævede felter: Host, Database, Login, Password, Port (5432)

**Insubiz API**

- Connection id: `insubiz_cloud_api`
- Bitwarden navn: `Insubiz Cloud API`
- Type: HTTP
- Krævede felter: Host + Extras
- Eksempel på extras:

```json
{
	"api_key": "x",
	"secret_key": "y"
}
```

## Schedule

Forsikringskontoret har adgang til UI'en i Airflow med rollen: `Vognpark` hvor de kun kan se de 3 DAGS som tilhører Vognpark. Her kan de selv trigger DAG'et

