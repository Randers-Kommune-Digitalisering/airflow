# Udddannels Stattistik KMD Insight Airflow DAG `README.md`
[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) | [**Konfiguration**](#konfiguration) | [**Afhængigheder**](#afhængigheder) | [**Schedule**](#schedule)

## Formål

Formålet med jobbet er at hente elevtrivselsdata fra uddannelsesstatistik API'et, transformere responsen til CSV og uploade filen til KMD Insight SFTP.

## Beskrivelse

Koden består af et DAG-job, der udfører følgende trin:

- Læser DAG-parametret `start_year` (integer)
  - Bruges til at beregne skoleårsvindue (fx `2025/2026`, `2024/2025`, ...)

- Læser Airflow Variablen `udddannels_stattistik_kmd_insight_file_path`
  - Skal indeholde sti + filnavn (fx `path/filename.csv`)

- Opretter hooks med faste connection ids:
  - `HttpHook(method="POST", http_conn_id="uddannelsesstatistik_api")`
  - `SFTPHook(ssh_conn_id="kmd_insight")`

- Henter API key fra `password` på HTTP-connection
  - Sender key som header: `Authorization: Bearer <api_key>`

- Kalder endpoint via POST:
  - `Api/v1/statistik`
  - Content-Type: `application/json`
  - Accept: `application/json`

- Bygger request payload for elevtrivsel:
  - `område=GS`, `emne=TRIV`, `underemne=TRIVIND`
  - `nøgletal=["Indikatorsvar"]`
  - Filtre for Randers, Folkeskoler, Udskoling/Mellemtrin, samt beregnede skoleår

- Validerer API-respons:
  - Kræver statuskode 2xx
  - Kræver gyldig JSON
  - Kræver at der findes data

- Transformerer nested JSON til flad tabel
  - Kolonner inkluderer bl.a. `År`, `Skolenavn`, `Trin` og indikatorfelter

- Eksporterer CSV i-memory med `;` separator og `utf-8`

- Uploader CSV til SFTP på stien fra `udddannels_stattistik_kmd_insight_file_path`

**Dataflow:**
- Uddannelsesstatistik API (HTTP POST) -> JSON transform -> CSV -> SFTP

## Konfiguration

### DAG params

- `start_year` (integer, default: `2020`)

### Airflow Variables

**Output filsti:**
- **Key:** `udddannels_stattistik_kmd_insight_file_path`
- Value: string med sti + filnavn (fx `exports/uddannelsesstatistik.csv`)

## Afhængigheder

:key: | **Airflow Connections**

**Uddannelsesstatistik API:**
- **`uddannelsesstatistik_api`**

**Conn Type**: HTTP

Bruges til at kalde API endpoint og hente data.

*Required felter*:
- Connection id
- Host (base URL)
- Password: API key (sendes som `Authorization: Bearer <api_key>`)

**SFTP destination:**
- **`kmd_insight`**

**Conn Type**: SFTP/SSH

Bruges til at skrive outputfilen til fjernsti.

*Required felter*:
- Connection id, Host, Login, Password/Key, Port


## Schedule
Schedule er sat op til at køre automatisk på følgende tidspunkter:

- **Tidspunkt:** `@monthly` (hver måned; den første ved midnat)