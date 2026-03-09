# Affaldterminalen Airflow DAG `README.md`
[**Formål**](#formål) | [**Dataflow**](#dataflow) | [**Beskrivelse**](#beskrivelse) | [**Excel-rapport (faner og logik)**](#excel-rapport-faner-og-logik) | [**Afhængigheder**](#afhængigheder) | [**Konfiguration**](#konfiguration) | [**Schedule**](#schedule)

## Formål
Jobbet henter og aggregerer affaldsregistreringer fra **ScanXNET** (MSSQL), genererer en Excel-rapport med flere faner (styret af `SHEET_SPECS`), og sender rapporten på email til Affaldsterminalen hver måned.

## Dataflow
**MSSQL (ScanXNET.dbo.Registration)** → **pandas (månedlig aggregering)** → **Excel (openpyxl, flere faner)** → **Email med vedhæftet fil**


1. Hent registreringer fra MSSQL (med `from_date`-filter) og aggreger pr. måned.
2. Transformér data pr. sheet (artikler, material-grupper, labels, (valgfrit) carrier og customer).
3. Skriv hver sheet som en tabel i **kg**, samt en aggregeret sektion i **tons** + procentvis ændring.
4. Send Excel-filen som attachment.


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


### Generisk Sheet-bygning (`SHEET_SPECS`)
Hver entry i `SHEET_SPECS` bliver til et worksheet:
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

### Pivot + layout
For hvert sheet:
1. Data transformeres til `CustomerKey`, `MaterialKey` og `CarrierKey` (sidstnævnte bruges især ved `group_by_carrier=True`).
2. Data pivoteres til 12 månedskolonner (Jan..Dec) + `YearTotal`.
3. Der skrives to sektioner i arket:
   - **KG-sektion** (månedskolonner + årssum + % ændring ift. forrige år pr. nøgle)
   - **TON-sektion** (aggregeret på tværs af customers, konverteret til tons, inkl. % ændring år-over-år)
4. Blank-linjer kan indsættes afhængigt af `blank_between` (`customer`, `material`, `customer_and_material`, `none`).
5. Sortering styres af `sort_mode` (`year_then_material` eller `material_then_year`).

## Afhængigheder

:key: | **Airflow Connections**

**MSSQL DB:**
- **`scanvaegt_db`**
- **Database: `ScanXNET`**
- **Tabel: `Registration`**

**Conn Type**: Microsoft SQL Server

- **Conn Type:** Microsoft SQL Server  
Bruges til at hente data fra `ScanXNET.dbo.Registration`.

*Required felter*:
  - Connection id, Host, Schema, Login, Password and Port(1433)

### Airflow Variables
**Affald email-konfiguration**
- **Key:** `affald_config`
- **Format (JSON):**
  - `sender_email`
  - `recipient_emails`

Eksempel:
```json
{
  "sender_email": "no-reply@randers.dk",
  "recipient_emails": ["modtager1@randers.dk", "modtager2@randers.dk"]
}
```

## Konfiguration
Excel-rapportens faner, labels og filtrering styres af:
- `affald_config.py`
  - `SHEET_SPECS`: faner, artikler, material-grupper, sortering, labels, carrier/customer-regler.
  - `GENBRUGSPLADSEN_CUSTOMER_NAMES`: default customer-navne

Carrier-logik:
- `sheet_specs_need_carrier()` returnerer True hvis en sheet-spec kræver carrier, fx:
  - `group_by_carrier=True`
  - `carrier_names` på sheet-niveau
  - `material_groups[*].carrier_names`

## Schedule
Schedule er sat op til at køre automatisk på følgende tidspunkter:

- **Tidspunkt:** Kl. 00:00 hver måned
