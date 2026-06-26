# Vognpark Synkronisere ændringer i Insubiz
[Formål](#formål) | [Beskrivelse](#beskrivelse) | [Inputkrav](#inputkrav) | [Output](#output) | [Afhængigheder](#afhængigheder) | [Schedule](#schedule)

## Formål

Formålet med denne DAG er at læse den nyeste Vognpark Excel-fil og synkronisere ændringer i Insubiz ved at afgangsføre og oprette køretøjer.

## Beskrivelse

**DAG:** `dag_vognpark_sync_changes`

Udfører følgende:

- Finder nyeste ulæste Vognpark Excel i Vognpark-postkassen.
- Læser fanen `Skal slettes` og finder køretøjer, der skal afgangsføres.
  - Køretøjer bliver ikke slettet fysisk i Insubiz.
  - Kun feltet `endDate` bliver opdateret for køretøjer, der skal afgangsføres.
- Læser fanen `Skal tilføjes` og bygger payloads til oprettelse af nye køretøjer.
- Opdaterer `endDate` på køretøjer i `Skal slettes`.
- Opretter nye køretøjer i Insubiz fra `Skal tilføjes`.


## Inputkrav

- Excel-filen skal sendes til `Vognpark-Postkassen`.
- Vedhæftet fil skal være `.xlsx` og have filnavn med prefix `uoverensstemmelser`.
- DAG læser kun ulæste mails (`UNSEEN`).
- Fanen `Skal tilføjes` skal have udfyldt `Customer_ID` for de køretøjer, der skal oprettes.

Hvis der ikke findes en relevant Excel-vedhæftning, fejler DAG med `AirflowFailException`.

## Output

Flowet opdaterer data direkte i Insubiz:

- Køretøjer i fanen `Skal slettes` får sat `endDate` til dags dato.
- Køretøjer i fanen `Skal tilføjes` oprettes i Insubiz.
- Resultat logges som antal afgangsførte og oprettede køretøjer.

## Afhængigheder

### Airflow Connections

**IMAP (Vognpark postkasse)**

- Connection id: `vognpark_imap`
- Bitwarden navn: `Postkasse - Vognpark`
- Krævede felter: Username (Login), Password

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

Schedule er sat op til at køre automatisk på følgende tidspunkter:

- `@monthly` Hver måned
