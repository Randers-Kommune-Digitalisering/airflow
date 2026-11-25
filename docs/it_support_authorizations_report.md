# IT support authorizations report

## Formål

DAG'en `it_support_authorizations_report` sender en Excelrapport over relevante autorisationer fra Logiva Signflow med oplysninger om tilhørende ansættelser og loginnavne i Delta.

## Beskrivelse

Signflow leverer sagsnummer, navn, CPR, LOS, fra-dato, handling og ledermail. Autorisationer med handlingen `Nyansat` medtages op til 14 dage før fra-datoen; andre handlinger medtages fra og med fra-datoen. Rapporten sorteres efter fra-dato.

Delta slås op på CPR, LOS og fra-dato for aktive ansættelser. Rapporten indeholder kolonnerne `Sagsnummer`, `Navn`, `CPR`, `LOS`, `Loginnavn(e)`, `Fra dato`, `Handling`, `Lederemail`, `Findes ikke i Delta` og `Kommentar`. Manglende match markeres med `x`. Kommentaren angiver, om der er en aktiv ansættelse under et andet LOS, en ansættelse der ikke er aktiv på fra-datoen, eller ingen ansættelse fundet for CPR på fra-datoen. Den viser ikke CPR-værdien.

Excel-filen skrives til den midlertidige mappe `sd_delta_sync` og sendes som vedhæftning til den modtagerliste, der er konfigureret i DAG'en. Rapporten indeholder personoplysninger; adgang til mail og midlertidige filer bør begrænses, og opbevaring/sletning bør afklares efter kommunens fælles processer.

**Dataflow:** Signflow → Delta-opslag → Excelrapport → e-mail.

## Afhængigheder

- Airflow Connection `logiva_signflow`: Signflow-host, login og password.
- Airflow Connection `delta_prod`: Delta-host, login, password og `token_url` i Extra. Brug dedikerede legitimationsoplysninger til løsningen med mindst mulige rettigheder.

## Schedule

Hverdage kl. 6:30 (`30 6 * * 1-5`, Europe/Copenhagen).
