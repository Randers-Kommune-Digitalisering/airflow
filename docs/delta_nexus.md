# Delta Nexus synkronisering
[**Formål**](#formål) | [**Dataflow**](#dataflow) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afhængigheder) | [**Schedule**](#schedule)

## Formål

Finde Nexus-brugere på baggrund af ændringer i Delta, opdatere deres organisationstilknytninger, identitetsfelter, primære organisation, standardleverandør og stillingsbetegnelse i Nexus og rapportere ændringerne.

## Dataflow

Delta engagementshistorik → aktive engagementer og brugere med Nexus-rolle → registrerings-ID'er, bruger-UUID'er og gyldighedsdato via Airflow til behandlings-DAG → person- og organisationsoplysninger fra Delta → opdatering af Nexus → dansk ændringsrapport via email.

Kun registrerings-ID'er, bruger-UUID'er og gyldighedsdato sendes mellem de to DAGs; personoplysninger hentes først i behandlings-DAG'en. Registrerings-ID'er opbevares også i Airflows kørselsmetadata for at undgå at udløse samme registrering igen.

## Beskrivelse

**`delta_nexus_polling`** venter om nødvendigt, indtil den seneste behandlingskørsel blev startet for mindst ti minutter siden, før den søger efter ændringer. Den foretager én `from`-forespørgsel fra det tidligste af seneste vellykkede datainterval og en time før det aktuelle dataintervals slutning. Kl. 04:00, 08:00, 12:00, 16:00 og 20:00 lokal tid foretager den `validFrom`-forespørgsler for dagens dato og eventuelle oversete datoer siden sidste vellykkede polling. Tidsstempler er i UTC, mens gyldighedsdatoen følger `Europe/Copenhagen` (CET/CEST). Pollingen fjerner dubletter på registrerings-ID og fravælger ID'er, der allerede findes i konfigurationen for en bevaret behandlingskørsel, også hvis behandlingen fejlede. Derfor bliver en registrering ikke udløst igen af en senere `validFrom`-forespørgsel, så længe Airflows kørselsmetadata bevares. Den finder aktive Nexus-produktionsrolle-brugere (ikke Review) pr. aktivt engagement. Hvis ingen findes, springes behandlingen over; ellers udløses én samlet `delta_nexus_processing`-kørsel med de valgte registrerings-ID'er, unikke bruger-UUID'er og gyldighedsdatoen. Pollingen venter på resultatet.

Uden en tidligere vellykket polling starter `from`-søgningen en time før dataintervallets slutning. `catchup=False` betyder, at en senere aktivering ikke automatisk gennemgår alle tidligere 10-minutters intervaller. Når der findes en tidligere vellykket kørsel, genlæses ændringer siden denne; oversete gyldighedsdatoer genlæses ved næste planlagte `validFrom`-tidspunkt. Registreringer uden Nexus-rolle kan genfindes i de efterfølgende pollinger, så længe de indgår i søgevinduet. `from`-søgningen kører stadig hvert tiende minut, også omkring midnat. Et task-retry efter fejl kan behandle en allerede påbegyndt batch igen; registrerings-ID'erne forhindrer nye udløsninger fra polling, ikke Airflows retries.

**`delta_nexus_processing`** henter de valgte brugeres personoplysninger, aktive engagementer, administrative enheder og jobfunktion fra Delta på gyldighedsdatoen. Nexus-klienten synkroniserer STS-organisationer og finder brugeren via ADGUID; STS-import eller tilknytning af en eksisterende professionel kan udføre API-kald med sideeffekter, og en ny professionel kan oprettes. Delta-organisationer, der ikke findes blandt aktive Nexus-organisationer, logges ved navn og beskrives i rapporten; de springes over uden at fejle brugerens behandling. Øvrige organisationstilknytninger opdateres i Nexus efter Delta-enhedernes UUID'er, mens rapportens navne for tilføjede organisationer hentes fra Nexus. Efter en opdatering genlæses brugerens Nexus-organisationer, før den primære organisation vælges og konfigurationen opdateres. Ændringer i identitetsfelter, stillingsbetegnelse og standardleverandør gemmes ligeledes i Nexus og beskrives i rapporten.

Behandlings-DAG'en sender en HTML-rapport på dansk med ét hovedpunkt pr. person og et underpunkt for hver foreslået ændring. Uændrede personer får punktet "Ingen ændringer". Hvis Nexus afviser oprettelse som dublet, gentages søgning og import én gang; ved endnu en dublet logges fejlen, og personen springes over uden at fejle kørslen. Rapporten sendes kun, når alle personer er behandlet uden fejl; fejlede forsøg sender ingen email, heller ikke ved genforsøg. Airflow-variablen `DELTA_NEXUS_SHOW_FAILED_ONLY` filtrerer rapporten til fejlede personer, når den er `true`. Da fejlede kørsler ikke sender email, giver `true` i praksis ingen rapport; `false` (standard) sender alle personer ved succes. Rapporten kan indeholde navne, brugernavne og UPN'er. Modtagerne angives i DAG-koden og skal være godkendte og adgangsbegrænsede. Anvend kommunens gældende processer for risikovurdering, sikker transport og opbevaring af emails, Airflow-logs og kørselsmetadata.

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

- **`delta_nexus_polling`:** `*/10 * * * *` (hvert 10. minut i `Europe/Copenhagen`, inklusive skift mellem CET og CEST); `validFrom` forespørges kl. 04:00, 08:00, 12:00, 16:00 og 20:00 lokal tid. Der er otte timer mellem kl. 20:00 og kl. 04:00. Gyldighedsdato følger lokal dato; `from`-tidsstempler sendes fortsat som UTC-instanter til Delta.
- **`delta_nexus_processing`:** Ingen selvstændig tidsplan; udløses kun af polling-DAG'en.
- Begge DAGs har startdato 9. november 2026 kl. 00:00 i `Europe/Copenhagen`, `catchup=False` og højst én aktiv kørsel. Polling har to retries med to minutters mellemrum; behandling har ét retry med 30 minutters mellemrum. Pollingens udløseropgave har dog nul retries og genstarter ikke en eksisterende behandlingskørsel. DAG'ene er sat til at være pauseret ved oprettelse.