# Nexus User Permission Sync `README.md`
[**Formål**](#formål) | [**Dataflow**](#dataflow) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afhængigheder) | [**Schedule**](#schedule)

## Formål
Sæt under-organisationer, Standardleverandør, unik id (dq-bruger), upn og stillingsbetegnelse (kun vikarer) i Nexus.

## Dataflow
Tjekker Delta for ændringer der træder i kræft på dato DAG kører og ændringer lavet på datoen men træder i kræft på en tidligere dato. Ændringer filtrers efter administrative organisationer og stillingstyper.

Brugerne fremsøges / importeres / oprettes i Nexus. herefter sættes organisationer, standardleverandør, unik id (dq-bruger), upn og stillingsbetegnelse (kun vikarer) i Nexus.

## Afhængigheder

:key: | **Airflow Connections**

**Delta API:**
- **`delta_prod`**

**Conn Type**: HTTP

Auth til Delta API

*Påkrævede felter*:
  - Connection id, Host, Login, Password og Extra (skal have token_url)

**Nexus:**
- **`nexus_prod`**
- **Bitwarden navn: `Nexus Randers Drift (client credentials)`**

**Conn Type**: HTTP

*Required felter*:
  - Connection id, Host, Login, Password, extra med token url og logout url

### Airflow Variables
**Nexus administrative organisationer**
- **Key:** `nexus_adm_org_dict`
- **Format (JSON):** `{<id>: [<id>, <id>], <id>: [], ...}`
- **Beskrivelse*:** Liste af id'er for adm. org. relateret til Nexus. Id'er er fra Delta (svarer til "syncId" i Nexus). Listen sættes af [Nexus Adm. Org. dict Airflow DAG ](./nexus_adm_org_dict.md)

**Stillingsbetegnelser i Delta**
- **Key:** `nexus_position_types_to_import`
- **Format (JSON):** [<stilling>, <stilling>, ...]
- **Beskrivelse*:** Liste af stillingsbetegnelser i Delta der skal importeres til Nexus

**Virkar stillingsbetegnelser**
- **Key:** `nexus_job_functions_to_import`
- **Format (JSON):** [<stilling>, <stilling>, ...]
- **Beskrivelse*:** Liste af stillingsbetegnelser (jobfunktioner) i Delta der skal importeres til Nexus. Disse sættes kun for vikarer. Stillingsbetegnelse sættes også i Nexus.

**Standardleverandøre**
- **Key:** `nexus_supplier_list`
- **Format (JSON):**
```
[
  {
    "nexus_name": <Nexus navn>
    "nexus_id": <Nexus id>
    "delta_id": <Delta id>
    "delta_name": <Delta navn>
  }, ...
]
```
- **Beskrivelse*:** Liste af standardleverandører til at mappe adm. org. fra Delta til en standardleverandør i Nexus

## Schedule
Schedule er sat op til at køre automatisk på følgende tidspunkter:

- **Tidspunkt:** */10 * * * * (hvert 10. minut)
