# SD Control and Error List Review Airflow DAG `README.md`

[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afhængigheder) | [**Schedule**](#schedule)

## Formål

Formålet med jobbet er at gennemgå fejl- og kontrolmeddelelser i SD Personaleweb for de konfigurerede afdelinger og markere de meddelelser, der har en tilladt kode, som `Set`.

Jobbet anvender RPA via Playwright. Logger ind i SD, åbner konteksten `Kg information` → `Fejl- & Kontrolmeddelelser`, gennemgår de valgte afdelinger og logger de koder, der er blevet markeret.

## Beskrivelse

Koden består af et DAG-job, der udfører følgende trin:

- Logger ind i SD Personaleweb.
- Åbner `Kg information` → `Fejl- & Kontrolmeddelelser`.
- Behandler afdelingerne i den rækkefølge, de står i konfigurationen:
	1. Ejendomsservice
	2. Personale
- Søger efter hver afdelingskode og åbner det første søgeresultat.
- Finder rammen med tabellen over fejl- og kontrolmeddelelser.
- Sætter tabellen til at vise 100 rækker(20 by default)
- Læser værdierne i tabellens `Kode`-kolonne.
- Sammenligner koderne med `ejendomservice_allowed_codes` og `personale_allowed_codes` listen.
- Markerer alle rækker med en tilladt kode som `Set`.

Koder, der findes i tabellen, men ikke er angivet i allow-listen, ændres ikke!


**Dataflow:**
	→ SD Personaleweb
	→ læste og markerede fejlkoder


## Afhængigheder

### Airflow Connections

**Loginoplysninger til SD Personaleweb:**

- **Connection id**: `sd_personaleweb`
- **Bitwarden navn**: `Robot_Personaleweb`

Connectionen bruges til login i SD Personaleweb.

*Required felter*:

- Connection id
- Username (Login)
- Password
- Host

### Airflow Variables

**Konfiguration af afdelinger og fejlkoder:**

- **Key**: `sd_control_error_list_config`

*Required felter*:

- `ejendomservice_department_codes`
- `ejendomservice_allowed_codes`
- `personale_department_codes`
- `personale_allowed_codes`

Variablen skal indeholde en liste over SD afdelingskoder og en liste over tilladte fejlkoder for hver afdelingstype:

```json
{
		"ejendomservice_department_codes": ["1000", "1001"],
		"ejendomservice_allowed_codes": ["KODE1", "KODE2"],
		"personale_department_codes": ["2000", "2001"],
		"personale_allowed_codes": ["KODE3", "KODE4"]
}
```

## Schedule

Jobbet er sat op til at køre automatisk på følgende tidspunkt:

- **Schedule:** `@weekly` (Hver uge)

```text
Ejendomsservice → Personale
```
