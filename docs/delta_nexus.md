# Delta Nexus synkronisering `README.md`
[**Formål**](#formål) | [**Dataflow**](#dataflow) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afhængigheder) | [**Schedule**](#schedule)

## Formål

Synkronisere Nexus-brugere og deres organisationstilknytninger, identitetsfelter, primære organisation, standardleverandør og stillingsbetegnelse på baggrund af ændringer i Delta.

## Dataflow

Delta engagementshistorik → aktive engagementer og brugere med Nexus-rolle → bruger-UUID'er via Airflow til behandlings-DAG → person- og organisationsoplysninger fra Delta → oprettelse eller opdatering i Nexus → lograpport via email.

Kun UUID'er og gyldighedsdato sendes mellem de to DAGs; personoplysninger hentes først i behandlings-DAG'en.

## Beskrivelse

**`delta_nexus_polling`** finder engagementer, der træder i kraft på relevante datoer, eller som er ændret siden sidste vellykkede polling. Den bruger UTC-datoen ved dataintervallets slutning som gyldighedsdato, fjerner dubletter og udvælger aktive brugere knyttet til aktive engagementer med en Nexus-produktionsrolle (ikke Review). Hvis der ikke findes brugere, springes behandlingen over. Ellers udløses `delta_nexus_processing` med en liste over bruger-UUID'er og gyldighedsdatoen. Pollingen venter på resultatet af behandlingen.

Ved første kørsel uden en tidligere vellykket polling starter søgningen ved det aktuelle dataintervals start. `catchup=False` betyder, at en senere aktivering ikke automatisk gennemgår alle tidligere 10-minutters intervaller.

**`delta_nexus_processing`** henter de valgte brugeres personoplysninger, aktive engagementer, administrative enheder og jobfunktion fra Delta på gyldighedsdatoen. Nexus' STS-organisationer synkroniseres, og brugeren findes via ADGUID eller importeres/oprettes fra STS. Organisationer tilføjes eller fjernes efter Delta, og identitetsfelter, stillingsbetegnelse, primær organisation og standardleverandør opdateres, når værdierne afviger. Primær organisation vælges blandt brugerens Nexus-organisationer.

Behandlings-DAG'en samler logbeskeder fra Nexus-klienten og sender de seneste 400 linjer som en HTML-rapport via Airflows konfigurerede email-backend. Rapporten kan indeholde navne, brugernavne og UPN'er. Modtagerne angives i DAG-koden og skal være godkendte og adgangsbegrænsede. Anvend kommunens gældende processer for risikovurdering, sikker transport og opbevaring af både emails og Airflow-logs.

## Afhængigheder

:key: | **Airflow Connections**

**Delta API:**
- **`delta_prod`**
- **Conn Type:** HTTP
- **Påkrævede felter:** Host, Login, Password og Extra med `token_url`.

**Nexus API:**
- **`nexus_prod`**
- **Conn Type:** HTTP
- **Påkrævede felter:** Host, Login, Password og Extra med `token_url`.

Airflows email-backend skal være konfigureret til at sende behandlingsrapporten. API-legitimationsoplysninger opbevares i Airflow Connections, ikke i DAG-koden.

## Schedule

- **`delta_nexus_polling`:** `*/10 * * * *` (hvert 10. minut, UTC).
- **`delta_nexus_processing`:** Ingen selvstændig tidsplan; udløses kun af polling-DAG'en.
- Begge DAGs har startdato 3. november 2026 kl. 00:00 UTC, `catchup=False`, højst én aktiv kørsel og to retries med to minutters mellemrum. De er sat til at være pauseret ved oprettelse.