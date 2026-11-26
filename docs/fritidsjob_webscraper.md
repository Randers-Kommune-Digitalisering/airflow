# Runtime-konfiguration for Fritidsjobs Webscraper

Denne DAG læser sin runtime-konfiguration fra Airflow Variable `fritidsjobs_webscraper_runtime_config` (JSON).

Denne dokumentation giver et overblik over:
- Påkrævede og valgfrie nøgler
- Anbefalede værdier til wait-indstillinger
- Eksempler på gyldig konfiguration

## Overordnet runtime-konfiguration

| Nøgle | Type | Påkrævet | Standard | Beskrivelse |
|---|---|---|---|---|
| `sender_email` | string | Ja | - | Afsender til e-mailnotifikationer. |
| `recipient_emails` | string / tuple(name, email) / array[string eller tuple(name, email)] | Ja | - | Modtagere af e-mailnotifikationer. |
| `smtp_server` | string | Ja | - | SMTP-host. |
| `sites` | array[object] | Ja | - | Konfiguration af sites, der skal scrapes. |

Eksempel på overordnet struktur:

```json
{
	"sender_email": "no-reply@example.com",
 	"recipient_emails": ["recipient@example.com"],
 	"smtp_server": "smtp.example.com",
	"sites": [
		{
			"site_name": "Eksempel Site",
			"site_url": "https://example.com/jobs",
			"lists": [
				{
					"list_name": "Eksempelliste",
					"list_elements": {
						"row": "div.job-row",
						"title": "h3",
						"link": "a"
					}
				}
			]
		}
	]
}
```

## Site-konfiguration

| Nøgle | Type | Påkrævet | Standard | Beskrivelse |
|---|---|---|---|---|
| `site_name` | string | Ja | - | Læsbart navn, som bruges i output/logning. |
| `site_url` | string | Ja | - | URL som indlæses af Playwright/Scrapy. |
| `lists` | array[object] | Ja | - | En eller flere liste-definitioner. |
| `allowed_domains` | array[string] | Nej | `[]` | Ekstra domæner, der må kaldes når request blocking er slået til. |
| `block_external_requests` | boolean | Nej | `true` | Standardadfærd for request blocking for alle lister på sitet. |

Bemærk:
- Sitets eget hostname er altid automatisk tilladt.
- En list-specifik `block_external_requests` overskriver værdien på site-niveau.

Eksempel på site-konfiguration:

```json
{
	"site_name": "Example Site",
	"site_url": "https://example.com/jobs",
	"allowed_domains": ["cdn.example.com"],
	"block_external_requests": true,
	"lists": [
		{
			"list_name": "Example Store",
			"list_elements": {
				"row": "table.jobs > tbody > tr",
				"title": "td:first-child a",
				"link": "td:first-child a"
			}
		}
	]
}
```

## Listekonfiguration

| Nøgle | Type | Påkrævet | Standard | Beskrivelse |
|---|---|---|---|---|
| `list_name` | string | Ja | - | Navn som bruges i output/logning. |
| `list_elements` | object | Ja | - | Feltselektorer til udtræk. |
| `list_route` | array | Nej | `[]` | Ordnede Playwright-handlinger før udtræk. |
| `allowed_domains` | array[string] | Nej | `[]` | Ekstra tilladte domæner kun for denne liste. |
| `block_external_requests` | boolean | Nej | Arver fra site (`true` hvis ikke sat) | Overskriver værdien på site-niveau for denne liste. |
| `extract_visible_rows_only` | boolean | Nej | `true` | Om skjulte rows skal udelades. |
| `wait_for_list_elements_timeout_ms` | int | Nej | `30000` | Timeout ved venten på udtrukne selektorer. |
| `wait_for_list_elements_state` | string | Nej | `"attached"` | Ventetilstand for udtrukne selektorer. |
| `wait_for_list_update_after_route` | boolean | Nej | `true` | Vent på selektorstabilitet efter route-handlinger. |
| `wait_for_list_update_selector` | string | Nej | Først `row`, ellers første wait-selector | Selector som bruges til at følge listeopdateringer. |
| `wait_for_list_update_stability_ms` | int | Nej | `800` | Hvor længe indhold skal være uændret. |
| `wait_for_list_update_poll_ms` | int | Nej | `150` | Poll-interval ved stabilitetstjek. |

Eksempel på listekonfiguration:

```json
{
	"list_name": "Example Store",
	"list_route": [
		{
			"wait_for": "select#region"
		},
		{
			"select": {
				"selector": "select#region",
				"label": "Midtjylland"
			}
		}
	],
	"wait_for_list_elements_state": "attached",
	"wait_for_list_elements_timeout_ms": 30000,
	"extract_visible_rows_only": true,
	"list_elements": {
		"row": "table.jobs > tbody > tr",
		"title": "td:first-child a",
		"link": "td:first-child a"
	}
}
```

## Nøgler i list_elements

| Nøgle | Type | Påkrævet | Beskrivelse |
|---|---|---|---|
| `row` | string | Nej, men stærkt anbefalet | Selector for hver row/item. Giver mere stabil kobling mellem titel/link. |
| `title` | string | Nej | Selector for title-tekst. |
| `link` | string | Nej | Selector for URL-kilde. |
| `frame` | string/object | Nej | Frame-scope til waits/udtræk. |
| `regex` | object | Nej | Row-filter baseret på tekstmatch. |

Bemærk: `list_elements` skal indeholde mindst én feltselector (typisk `title` og/eller `link`) for at kunne udtrække items.

`regex`-objekt:

| Nøgle | Type | Påkrævet | Beskrivelse |
|---|---|---|---|
| `selector` | string | Ja (når `regex` bruges) | Selector som leverer teksten, der skal matches. |
| `pattern` | string | Ja (når `regex` bruges) | Regex-mønster, fx `".*Randers.*"`. |


Eksempel på listelementkonfiguration:

```json
{
	"row": "table.job-table > tbody > tr",
	"title": "td:first-child a",
	"link": "td:first-child a",
	"regex": {
		"selector": "td:nth-child(2)",
		"pattern": ".*Randers.*"
	}
}
```

## Formater for list_route-steps

Du kan blande disse step-typer i `list_route`:

| Step-type | Eksempel | Beskrivelse |
|---|---|---|
| String | `"button.show-jobs"` | Behandles som click-selector. |
| Objekt med `click` | `{ "click": "button.show-jobs" }` | Klikker selector. |
| Objekt med `wait_for` | `{ "wait_for": "table.jobs" }` | Venter på selector i nuværende scope. |
| Objekt med `frame` | `{ "frame": "iframe#jobs" }` | Skifter scope til frame. |
| Objekt med `select` | `{ "select": { "selector": "select#region", "label": "Randers" } }` | Vælger option i `<select>`. |
| Array af strings | `["button.open", "button.filter"]` | Klikker hver selector i rækkefølge. |

Nøgler i `select`-objekt:

| Nøgle | Type | Påkrævet | Beskrivelse |
|---|---|---|---|
| `selector` | string | Ja | CSS-selector til `<select>`-elementet. |
| `value` | string | Nej | Vælger option via `value`. |
| `label` | string | Nej | Vælger option via tekst/label. |
| `index` | int | Nej | Vælger option via indeks. |

`frame`-værdi kan være:

| Format | Eksempel | Beskrivelse |
|---|---|---|
| string | `"iframe#jobs"` | Finder frame via CSS-selector. |
| object.selector | `{ "selector": "iframe#jobs" }` | Finder frame via CSS-selector. |
| object.url_contains | `{ "url_contains": "hr-manager.net" }` | Finder frame via URL-delmatch. |
| object.name | `{ "name": "jobs-frame" }` | Finder frame via `name` attribut. |

Eksempel på `list_route`:

```json
[
	{ "frame": "iframe#jobs" },
	{ "wait_for": "select#region" },
	{
		"select": {
			"selector": "select#region",
			"label": "Region Midtjylland"
		}
	},
	{ "wait_for": "table.jobs" }
]
```

## Wait-indstillinger: hvornår bruges hvad

### `wait_for_list_elements_state`

| Værdi | Hvornår bruges den | Anbefaling |
|---|---|---|
| `"attached"` | Elementet skal blot findes i DOM. | Anbefalet standard for de fleste sites. |
| `"visible"` | Elementet findes, men er skjult indtil rendering/filtrering er færdig. | Brug hvis udtræk starter for tidligt. |
| `"detached"` | En selector skal forsvinde fra DOM. | Bruges sjældent. |
| `"hidden"` | En selector skal blive skjult. | Bruges sjældent. |

Praktisk anbefaling:
- Start med `"attached"`.
- Skift til `"visible"` hvis udtræk starter for tidligt, mens rows stadig er skjulte.
- Undgå `"detached"` og `"hidden"` for normale row/title/link-selectorer.

### Øvrige `wait_for_*`-nøgler

| Nøgle | Hvornår bør den justeres | Typisk startværdi |
|---|---|---|
| `wait_for_list_elements_timeout_ms` | Ved langsomme sider eller tunge filtre. | `30000` |
| `wait_for_list_update_after_route` | Holdes `true` når listeindhold opdateres asynkront efter click/select. | `true` |
| `wait_for_list_update_selector` | Når automatisk selector-valg er støjende eller ustabilt. | `row`-selector |
| `wait_for_list_update_stability_ms` | Når indhold flakker eller opdateres i flere bølger. | `800` |
| `wait_for_list_update_poll_ms` | Når polling skal være mindre hyppig. | `150` |

## extract_visible_rows_only

| Nøgle | Standard | Effekt | Sæt til `false` når |
|---|---|---|---|
| `extract_visible_rows_only` | `true` | Markerer rows som visible/hidden og udelader hidden rows ved udtræk. | Sitet holder gyldige rows skjult, eller visibility-heuristikken fjerner legitime rows. |

## block_external_requests

| Nøgle | Standard | Effekt | Sæt til `false` når | Anbefaling |
|---|---|---|---|---|
| `block_external_requests` | `true` | Kun sitets host og `allowed_domains` tillades. | Et site afhænger af mange eksterne domæner, og en stabil allow-list ikke kan vedligeholdes. | Behold `true` når muligt for bedre isolation og færre irrelevante tredjepartsrequests. |

## Eksempel på site-konfiguration

```json
{
	"site_name": "Rema 1000",
	"site_url": "https://job.rema1000.dk/ledige-stillinger",
	"allowed_domains": ["hr-manager.net"],
	"block_external_requests": true,
	"lists": [
		{
			"list_name": "Randers",
			"list_route": [
				{ "frame": "iframe#iFrameResizer0" },
				{ "wait_for": "select#location_ddfilter" },
				{
					"select": {
						"selector": "select#location_ddfilter",
						"label": "Region Midtjylland"
					}
				},
				{ "wait_for": "select#location_ddfiltercascade1" },
				{
					"select": {
						"selector": "select#location_ddfiltercascade1",
						"label": "Randers"
					}
				}
			],
			"wait_for_list_elements_state": "attached",
			"wait_for_list_elements_timeout_ms": 30000,
			"wait_for_list_update_after_route": true,
			"wait_for_list_update_stability_ms": 800,
			"wait_for_list_update_poll_ms": 150,
			"extract_visible_rows_only": true,
			"list_elements": {
				"row": "div[role='listitem']",
				"title": "div.project-title",
				"link": "div.project-title"
			}
		}
	]
}
```

**OBS**: Valgfrie nøgler fra dette eksempel kan udelades; her sættes de blot til deres default-værdier til demonstration.