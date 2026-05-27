# Affaldterminalen Airflow DAG `README.md`
[**Formål**](#formål) | [**Dataflow**](#dataflow) | [**Beskrivelse**](#beskrivelse) | [**Excel-rapport (faner og logik)**](#excel-rapport-faner-og-logik) | [**Afhængigheder**](#afhængigheder) | [**Konfiguration**](#konfiguration) | [**Schedule**](#schedule)

## Formål

Jobbet består af 2 dele:

Første del henter og aggregerer affaldsregistreringer fra **ScanXNET** (MSSQL), genererer en Excel-rapport med flere faner (styret af `sheet_specs` fra affald_report_config variable), og sender rapporten på email til Affaldsterminalen hver måned.

Anden del henter mængder fra **Marius Pedersen API** og aggregerer data pr. måned (feltet `taxesQuantity` i kg), genererer en separat Excel-fil.

## Dataflow
**ScanXNET-flow:**
**MSSQL (ScanXNET.dbo.Registration)** → **pandas (månedlig aggregering)** → **Excel (openpyxl, flere faner)** → **Email med vedhæftet fil**

1. Hent registreringer fra MSSQL (med `from_date`-filter) og aggreger pr. måned.
2. Transformér data pr. sheet (artikler, material-grupper, labels, (valgfrit) carrier og customer).
3. Skriv hver sheet som en tabel i **kg**, samt en aggregeret sektion i **tons** + procentvis ændring.
4. Send Excel-filen som attachment.

**Marius Pedersen-flow:**
**MP API (HTTP POST)** → **pandas/aggregation (sum pr. måned)** → **Excel (openpyxl, 1 fane)** → → **Email med vedhæftet fil**

1. Beregn periode for udtræk (fra 1. januar sidste år til køre-dato i DAG timezone).
2. Aggreger data pr. (customerNumber, activityYear, activityMonth) på `taxesQuantity` (kg).
3. Send Excel-filen som attachment.

Kørslen sender 2 vedhæftninger:
1. Affaldsterminalen-rapport (ScanXNET)
2. Marius Pedersen-udtræk (MP API)

## Excel-rapport (faner og logik)
Excel bygges i `dags/dag_affald/affald_data.py` med `openpyxl`.

### Datagrundlag (SQL)
`fetch_affald_registration_monthly_df()` returnerer månedligt aggregerede rækker med:
- `CustomerName`
- `ArticleNumber`
- `year_month` (format `YYYY-MM`)
- `weightnet_sum` (SUM af `WeightNet` som float)
- samt **`CarrierName`** hvis `include_carrier=True` (tom/blank bliver til `Ukendt`)

Standardadfærd i funktionen:
- `from_date` default: `2019-01-01`
- `customer_names`:
  - `None` ⇒ filter til `GENBRUGSPLADSEN_CUSTOMER_NAMES`
  - `[]` ⇒ **ingen filter** (alle customers)
- `article_numbers`:
  - `None` ⇒ default artikler udledt af `SHEET_SPECS` (inkl. `material_groups`)
  - `[]` ⇒ **ingen filter** (alle artikler)
- `carrier_names` kan bruges som IN-filter **kun hvis** `include_carrier=True`.


### Generisk Sheet-bygning (`sheet_specs`)
Hver entry i `affald_report_config` bliver til et worksheet:
- Der udvælges artikler pr. sheet:
  - Enten `spec["articles"]`, eller (hvis udeladt) udledt fra `material_groups[*].articles`.
- Der kan anvendes:
  - `customer_names` (None betyder “alle” på sheet-niveau)
  - `customer_label` (kollapser alle customers til én label, fx “Randers”)
  - `carrier_names` og/eller `carrier_label`
  - `material_groups` (map ArticleNumber til et “MaterialKey” label)
    - pr. gruppe kan der desuden være `carrier_names` og `customer_names` som ekstra filter for mapping
  - `drop_unmapped_group_rows` (fjerner “rest”-rækker for artikler der indgår i `material_groups`, men ikke blev mappet)
  - `auto_append_vare_nr` (default True): tilføjer automatisk “(vare nr. ...)” til group-labels, hvis ikke label allerede indeholder “vare nr”.
  - `sort_mode` (`year_then_material` eller `material_then_year`)
  - `blank_between` (`customer`, `material`, `customer_and_material`, `none`)

### Pivot + layout
For hvert sheet:
1. Data transformeres til `CustomerKey`, `MaterialKey` og `CarrierKey` (sidstnævnte bruges især ved `group_by_carrier=True`).
2. Data pivoteres til 12 månedskolonner (Jan..Dec) + `YearTotal`.
3. Der skrives to sektioner i arket:
   - **KG-sektion** (månedskolonner + årssum + % ændring ift. forrige år pr. nøgle)
   - **TON-sektion** (aggregeret på tværs af customers, konverteret til tons, inkl. % ændring år-over-år)
4. Blank-linjer kan indsættes afhængigt af `blank_between` (`customer`, `material`, `customer_and_material`, `none`).
5. Sortering styres af `sort_mode` (`year_then_material` eller `material_then_year`).

### Marius Pedersen (MP) logik

([Marius Pedersen API Dokumentation](https://github.com/Randers-Kommune-Digitalisering/dev-docs-library/blob/main/vidensdeling/ressourcer/api/Marius%20Pedersen%20API/API%20Dokumentation.pdf))

- MP-data hentes via Airflow HTTP connection `marius_pedersen_api`
- API key forventes at ligge i connection “Password” (sendes som header `MP_ApiKey`)
- Endpoint der kaldes:
  - `POST /umbraco/api/wastestatistic/GetWasteamountStatistic`
- Data aggregeres pr. (customerNumber, activityYear, activityMonth) på feltet `taxesQuantity` (kg)
- Perioden der hentes for:
  - `from_date`: 1. januar sidste år (ifht. køre-dato)
  - `to_date`: køre-dato (logical date i DAG timezone)

## Afhængigheder

:key: | **Airflow Connections**

**MSSQL DB:**
- **`scanvaegt_db`**
- **Bitwarden navn: `SQL Bruger til ScanxNet - Affald UMT`**
- **Database: `ScanXNET`**
- **Tabel: `Registration`**

**Conn Type**: Microsoft SQL Server

- **Conn Type:** Microsoft SQL Server  
Bruges til at hente data fra `ScanXNET.dbo.Registration`.

*Required felter*:
  - Connection id, Host, Schema, Login, Password and Port(1433)

**Marius Pedersen(MP) API :**
- **`marius_pedersen_api`**
- **Bitwarden navn: `Marius Pedersen(MP) API`**

**Conn Type**: HTTP

Bruges som Connection id i Airflow til at hente host og APi Key til MP API'et

*Required felter*:
  - Connection id, Host, og Password

### Airflow Variables
**Affald email-konfiguration**
- **Key:** `affald_runtime_config`
- **Format (JSON):**
  - `sender_email`
  - `recipient_emails`
  - `smtp_server`

Eksempel:
```json
{
  "sender_email": "no-reply@randers.dk",
  "recipient_emails": ["modtager1@randers.dk", "modtager2@randers.dk"],
  "smtp_server": "xx"
}
```

**Affald report config som bruges til vedligeholdelse af data fra Scanvægt**
- **Key:** `affald_report_config`
- **Format (JSON):**
  - `genbrugspladsen_customer_names`
  - `sheet_specs`

Eksempel:
```json
{
  "genbrugspladsen_customer_names": [
    "Genbrugspladsen 1",
    "Genbrugspladsen 2"
  ],
  "sheet_specs": [
    {
      "sheet_name": "Pap",
      "title": "Pap til genbrug - mængder genbrugspladser hele året",
      "articles": ["xx"]
    }
  ]
}
```

## Konfiguration
Excel-rapportens faner, labels og filtrering styres af:
- Airflow Variablen: `affald_report_config`
  - `sheet_specs`: faner, artikler, material-grupper, sortering, labels, carrier/customer-regler.
  - `genbrugspladsen_customer_names`: default customer-navne

Carrier-logik:
- `sheet_specs_requires_carrier()` returnerer True hvis en sheet-spec kræver carrier, fx:
  - `group_by_carrier=True`
  - `carrier_names` på sheet-niveau
  - `material_groups[*].carrier_names`

## Schedule
Schedule er sat op til at køre automatisk på følgende tidspunkter:

- **Tidspunkt:** Kl. 07:00 hver 5. dag på måneden
