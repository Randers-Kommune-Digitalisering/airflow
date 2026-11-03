# Delta Nexus synkronisering
[**Formål**](#formål) | [**Dataflow**](#dataflow) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afhængigheder) | [**Schedule**](#schedule)

## Formål

Finde Nexus-brugere på baggrund af ændringer i Delta og rapportere foreslåede ændringer i deres organisationstilknytninger, identitetsfelter, primære organisation, standardleverandør og stillingsbetegnelse. Oprettelse og opdatering af Nexus-konfiguration er foreløbig slået fra.

## Dataflow

Delta engagementshistorik → aktive engagementer og brugere med Nexus-rolle → registrerings-ID'er, bruger-UUID'er og gyldighedsdato via Airflow til behandlings-DAG → person- og organisationsoplysninger fra Delta → kontrol af foreslåede Nexus-ændringer → dansk ændringsrapport via email.

Kun registrerings-ID'er, bruger-UUID'er og gyldighedsdato sendes mellem de to DAGs; personoplysninger hentes først i behandlings-DAG'en. Registrerings-ID'er opbevares også i Airflows kørselsmetadata for at undgå at udløse samme registrering igen.

## Beskrivelse

**`delta_nexus_polling`** venter om nødvendigt, indtil den seneste behandlingskørsel blev startet for mindst ti minutter siden, før den søger efter ændringer. Den foretager én `from`-forespørgsel fra det tidligste af seneste vellykkede datainterval og en time før det aktuelle dataintervals slutning. For hver lokal dato, der er passeret siden sidste vellykkede polling, foretager den én `validFrom`-forespørgsel; i den første time efter lokal midnat forespørges den aktuelle dato også. Tidsstempler er i UTC, mens gyldighedsdatoen følger `Europe/Copenhagen` (CET/CEST). Pollingen fjerner dubletter på registrerings-ID og fravælger ID'er, der allerede findes i en behandlingskørsels konfiguration, også hvis behandlingen fejlede. Den finder aktive Nexus-produktionsrolle-brugere (ikke Review) pr. aktivt engagement. Hvis ingen findes, springes behandlingen over; ellers udløses én samlet `delta_nexus_processing`-kørsel med de valgte registrerings-ID'er, unikke bruger-UUID'er og gyldighedsdatoen. Pollingen venter på resultatet.

Uden en tidligere vellykket polling starter `from`-søgningen en time før dataintervallets slutning. `catchup=False` betyder, at en senere aktivering ikke automatisk gennemgår alle tidligere 10-minutters intervaller. Når der findes en tidligere vellykket kørsel, genlæses ændringer siden denne og alle mellemliggende gyldighedsdatoer. Registreringer uden Nexus-rolle kan genfindes i de efterfølgende pollinger, så længe de indgår i søgevinduet.

**`delta_nexus_processing`** henter de valgte brugeres personoplysninger, aktive engagementer, administrative enheder og jobfunktion fra Delta på gyldighedsdatoen. Nexus-klienten synkroniserer STS-organisationer og finder brugeren via ADGUID; STS-import eller tilknytning af en eksisterende professionel kan stadig udføre API-kald med sideeffekter. Oprettelse af en ny professionel springes over. Foreslåede ændringer i organisationer, identitetsfelter, stillingsbetegnelse, primær organisation og standardleverandør beskrives i rapporten som ændringer, men kald til opdatering af organisationer og konfiguration er slået fra. En eksisterende primær organisation vælges fra brugerens Nexus-organisationer.

Behandlings-DAG'en sender en HTML-rapport på dansk med ét hovedpunkt pr. person og et underpunkt for hver foreslået ændring. Uændrede personer får punktet "Ingen ændringer". Rapporten sendes kun, når alle personer er behandlet uden fejl; fejlede forsøg sender ingen email, heller ikke ved genforsøg. Airflow-variablen `DELTA_NEXUS_SHOW_FAILED_ONLY` filtrerer rapporten til fejlede personer, når den er `true`. Da fejlede kørsler ikke sender email, giver `true` i praksis ingen rapport; `false` (standard) sender alle personer ved succes. Rapporten kan indeholde navne, brugernavne og UPN'er. Modtagerne angives i DAG-koden og skal være godkendte og adgangsbegrænsede. Anvend kommunens gældende processer for risikovurdering, sikker transport og opbevaring af emails, Airflow-logs og kørselsmetadata.

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
- Begge DAGs har startdato 3. november 2026 kl. 00:00 UTC, `catchup=False`, højst én aktiv kørsel og som udgangspunkt to retries med to minutters mellemrum. Pollingens udløseropgave har dog nul retries og genstarter ikke en eksisterende behandlingskørsel. DAG'ene er sat til at være pauseret ved oprettelse.