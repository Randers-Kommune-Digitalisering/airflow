# X-Flow-værdilister Airflow DAG `README.md`

[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afhængigheder) | [**Schedule**](#schedule)

## Formål

DAG'en erstatter indholdet af konfigurerede værdilister i X-Flow med værdier fra Airflow-konfigurationen, andre Airflow-variabler eller godkendte HTTPS-kilder.

## Beskrivelse

DAG'en kører dagligt og udfører følgende trin:

- Læser runtime-konfigurationen fra Airflow-variablen `xflow_value_lists_config`.
- Validerer konfigurationen, værdilisterne og alle datakilder, inden der sendes opdateringer til X-Flow.
- Henter værdier fra én af tre kilder: værdier direkte i konfigurationen, en anden Airflow-variabel eller en JSON-fil fra en godkendt HTTPS-vært.
- Erstatter hver konfigureret værdiliste via X-Flow API'et. DAG'en henter ikke den eksisterende liste og fletter ikke værdier sammen med den nye liste.
- Opretter hvert listeelement med en fortløbende `key` og `indeks`. Værdien fra `created_by` angives som `oprettetAf`.

**Dataflow:**
- Airflow-variabel / godkendt HTTPS-kilde → validerede værdier → PUT til X-Flow-værdiliste

**Bemærk:**
- Kilden skal være en ikke-tom JSON-liste med strenge, der ikke kun består af mellemrum. Tomme lister kan derfor ikke bruges til at rydde en værdiliste.
- Rækkefølgen afgør de fortløbende `key`- og `indeks`-værdier. Kontrollér i et testmiljø, hvordan ændringer af nøgler påvirker eventuelle referencer til værdilisten.
- Alle kilder valideres før første opdatering. Opdateringer af flere værdilister er dog ikke en samlet transaktion. Hvis en senere opdatering fejler, kan tidligere lister allerede være erstattet. En genkørsel sender de fulde lister igen.
- URL-kilder skal bruge HTTPS på port 443 uden loginoplysninger, query string eller fragment. Omdirigeringer og IP-adresser afvises. Begræns også udgående netværkstrafik med en netværksbaseret allow-list.
- Brug kun ikke-følsomme opslagstekster. Medtag ikke personoplysninger eller hemmeligheder i konfigurationen eller kilderne. DAG'en logger ikke værdierne eller API-tokenet.

## Afhængigheder

### Airflow Connections
:key: | **Airflow Connections**

**X-Flow API:**
- **Connection ID:** `xflow`
- **Connection Type:** `HTTP`

Forbindelsen bruges til at kalde X-Flow API'et.

*Påkrævede felter:*
- **Host:** Den fulde API-basisadresse, f.eks. `https://api.<instance>.xflow.dk/`, uden sti eller query string.
- **Password:** X-Flow public API-token. Opbevar tokenet i den godkendte hemmelighedsløsning, og begræns dets rettigheder til de nødvendige værdilister. Brug helst et særskilt token til jobbet.

### Airflow Variables
:key: | **Airflow Variables**

**Runtime-konfiguration for X-Flow-værdilister:**
- **Key:** `xflow_value_lists_config`

Variablen indeholder listen over værdilister, deres datakilder og den afsender, der registreres på de enkelte listeelementer.

Eksempel:
```json
{
  "created_by": "Digitalisering",
  "allowed_url_hosts": ["raw.githubusercontent.com"],
  "sync": [
    {
      "list_id": "f00f8e6e-13c4-48ef-a36c-6c238910a543",
      "origin": "values",
      "values": ["one", "two"]
    },
    {
      "list_id": "80362131-e79e-44f6-818a-acdd5fca9ce6",
      "origin": "url",
      "url": "https://raw.githubusercontent.com/organization/repository/main/list.json"
    },
    {
      "list_id": "27aa9285-042b-4bdd-994e-9576701a2e5e",
      "origin": "airflow",
      "airflow": "source_values_variable"
    }
  ]
}
```

- `created_by` er påkrævet og bruges som værdien for `oprettetAf` på hvert listeelement.
- `allowed_url_hosts` indeholder præcise DNS-værtsnavne, ikke URL-adresser eller origins. Angiv `[]`, hvis der ikke bruges URL-kilder.
- `sync` indeholder en eller flere værdilister. Hver `list_id` skal være værdilistens UUID i X-Flow.
- `origin` kan være `values`, `url` eller `airflow`. Hvert element skal have præcis det tilsvarende kildefelt: `values`, `url` eller `airflow`.
- Ved `values` angives værdierne direkte som en JSON-liste af strenge.
- Ved `url` skal svaret være en JSON-liste af strenge på højst 1 MB. URL'ens værtsnavn skal være med i `allowed_url_hosts`.
- Ved `airflow` angiver `airflow` navnet på en anden Airflow-variabel, der indeholder en JSON-liste af strenge.
- Erstat eksempel-UUID'erne og kildeoplysningerne med de værdier, der gælder for installationen.

### Eksterne Python-pakker

DAG'en bruger `requests`, som allerede indgår i projektets afhængigheder. Der tilføjes ingen nye Python-pakker til denne integration.

## Schedule

DAG'en kører automatisk én gang dagligt:

- **Tidspunkt:** Kl. 06.00 i tidszonen `Europe/Copenhagen`.
- **Cron-syntaks:** `0 6 * * *`

Kør først DAG'en manuelt mod testværdilister, og kontrollér værdier og rækkefølge i X-Flow, før den planlagte kørsel aktiveres i produktion.
