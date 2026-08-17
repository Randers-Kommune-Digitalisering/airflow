# SD Fleksjobrefusion Airflow DAG `README.md`

[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afhængigheder) | [**Schedule**](#schedule)

## Formål

Formålet med jobbet er at automatisere registreringen af fleksjobrefusioner i SD Personaleweb.

Jobbet henter den nyeste Excel-vedhæftning fra Fleksjobrefusion postkassen, udtrækker medarbejdernes tjenestenummer, institution, beløb og lønart og anvender derefter browserautomatisering via Playwright til at logge ind i SD og behandle hver medarbejder i Personaleweb.

Efter gennemførslen sendes en email med enten:

- En succesmeddelelse, hvis alle medarbejdere er behandlet uden fejl
- En fejlopsummering med de medarbejdere, der ikke kunne behandles

## Beskrivelse

Koden består af et DAG-job, der udfører følgende trin:

- Finder den nyeste Excel-vedhæftning i postkassen, hvor:
  - filtypen er `.xlsx`
  - filnavnet skal starte med `Fleksjobrefusion`
- Læser Excel-filen og finder oplysninger om medarbejderne fra kolonnerne:
  - `TJNR.`
  - `Int.`
  - `Lønart 684`
  - `Lønart 685`
- Udvælger rækker, hvor der er angivet et beløb:
  - Beløb fra `Lønart 684` anvendes først
  - Hvis `Lønart 684` ikke er udfyldt, anvendes `Lønart 685`
- Åbner SD Personaleweb:
  - Logger ind
  - Vælger `Randers Kommune`
  - Åbner Personaleweb
  - Går til `Indberetning` → `Merarbejde`
- Finder hver medarbejder ved hjælp af tjenestenummer og institution.
- Indtaster beløb og lønart for hver medarbejder.
- Marker medarbejderen som godkendt.
- Fortsætter med næste medarbejder, hvis en medarbejder ikke kan behandles.
- Sender en email med resultatet:
  - Ved succes sendes antal behandlede medarbejdere og navnet på Excel-filen
  - Ved fejl sendes en liste over de medarbejdere, der ikke kunne behandles
- Lukker SD Personaleweb, når behandlingen er færdig.

**Dataflow:**

- Fleksjobrefusion-postkasse (IMAP) + Excel-vedhæftning
  → medarbejder- og beløbsdata
  → SD Personaleweb
  → succes- eller fejlmail

**Inputfilens format:**

Excel-filen skal indeholde følgende kolonner:

| Kolonne | Beskrivelse |
| --- | --- |
| `TJNR.` | Medarbejderens tjenestenummer |
| `Int.` | Institution eller organisatorisk enhed |
| `Lønart 684` | Beløb for lønart 684 |
| `Lønart 685` | Beløb for lønart 685 |

Der behandles kun rækker, hvor enten `Lønart 684` eller `Lønart 685` indeholder en værdi.


**Email ved succes:**

Ved en succesfuld kørsel sendes en email med:

- Emne: `SD Fleksjobrefusion: Kørsel gennemført uden fejl`
- Antal behandlede medarbejdere
- Navnet på den anvendte Excel-fil

**Email ved fejl:**

Hvis en eller flere medarbejdere fejler, sendes en email med:

- Emne, der angiver antallet af fejlede medarbejdere
- Tjenestenummer
- Institution
- Beløb
- Lønart

**Forudsætning (manuel proces):**

Personale og HR lægger en Excel-liste i Fleksjobrefusion - postkassen.
Filnavnet skal starte med `Fleksjobrefusion` for at blive fundet af jobbet.

## Afhængigheder

### Airflow Connections
:key: | **Airflow Connections**

**Login oplysninger til SD PersonaleWeb:**
- **`sd_fleksjobrefusion_personaleweb`**
- **Bitwarden navn: `Robot_Personaleweb`**

Bruges til at hente login/password til SD Fleksjobrefusion Personaleweb

*Required felter*:
	- Connection id, Username (Login) og Password

**IMAP (Postkasse til Fleksjobrefusion):**
- **`Fleksjobrefusion_imap`**
- **Bitwarden navn: `Postkasse - Fleksjobrefusion`**

Bruges til at hente login/password til Fleksjobrefusion postkassen, som DAG'en læser input fra.

*Required felter*:
	- Connection id, Username (Login) og Password

### Airflow Variables
:key: | **Airflow Variables**

**Fleksjobrefusion Runtime Konfiguration (IMAP):**
- **Key**: `fleksjobrefusion_runtime_config`

*Required felter*:
	- `imap_server`

Eksempel:
```json
{
	"imap_server": "imap.example.local"
}
```

## Schedule

Personale og HR har adgang til UI'en i Airflow med rollen: `Fleksjobrefusion` hvor de kun kan se det DAG som tilhører Fleksjobrefusion. Her kan de selv trigger DAG'et efter eget behov.