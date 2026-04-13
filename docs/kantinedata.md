# Kantinedata Airflow DAG `README.md`
[**Formål**](#formål) | [**Beskrivelse**](#beskrivelse) | [**Afhængigheder**](#afhængigheder) | [**Schedule**](#schedule)

## Formål

Formålet med jobbet er at overføre XML-filer fra kantinedata postkassen til kantinedata SFTP. 

For at understøtte den faglige arbejdsgang, kræver jobbet:
- Automatisk videresendelse af relevante emails til kantinedata postkassen.
- Automatisk overførsel af filer fra SFTP til fællesdrev.
- Manuel indlæsning i fagsystem fra fællesdrev.

## Beskrivelse

Koden består af et DAG-job, der udfører følgende trin:

1. Opretter forbindelse til IMAP (postkasse) og SFTP.
2. Finder alle ulæste emails (IMAP search: `UNSEEN`) i `INBOX`.
3. For hver ulæst email:
    - Henter email-indholdet uden at markere den som læst (`BODY.PEEK[]`).
    - Gennemgår alle vedhæftninger og filtrerer til XML (`application/xml` eller `text/xml`).
    - Validerer at hver XML-vedhæftning kan parses (well-formed XML).
    - Uploader hver gyldig XML-vedhæftning til SFTP med et filnavn i formatet `EksporteredeOrdrer_XX.xml`.
4. Markerer kun en email som læst (IMAP flag `\\Seen`) hvis:
    - Emailen indeholder mindst én XML-vedhæftning, og
    - Alle fundne XML-vedhæftninger blev uploadet uden fejl.
5. Hvis der findes én eller flere ulæste emails uden nogen XML-vedhæftninger, fejler kørslen til sidst med en fejl.

**Filnavne på SFTP**
- Filnavne tildeles dynamisk ved upload ved at kigge på eksisterende filer i den aktuelle SFTP-mappe (`listdir(".")`).
- Der er 10 “slots”: `EksporteredeOrdrer_01.xml` … `EksporteredeOrdrer_10.xml`.
- Hvis alle 10 filnavne allerede findes på SFTP, fejler kørslen.

**Fejlhåndtering (kort)**
- IMAP search-fejl (`status != OK`) logger en advarsel og afslutter uden at fejle.
- Emails uden XML-vedhæftninger udløser en fejl til sidst (for at gøre opmærksom på uventet indhold).
- Ugyldig XML (parse-fejl) bliver sprunget over og emailen bliver ikke markeret som læst; der rejses ikke automatisk en samlet fejl kun pga. ugyldig XML.

**Dataflow:**
- XML-vedhæftninger fra kantinedata postkasse → lagres på SFTP-server.

## Afhængigheder

:key: | **Airflow Connections**

**Postkasse:**
- **`kantinedata_imap`**  

  **Conn Type**: IMAP

  Bruges som `Connection id` i Airflow til at hente host, bruger, adgangskode, port og extra JSON til IMAP-forbindelsen

  *Required felter*:
  - Connection id, Host, Login, Password, Port (143) og Extra

  Extra JSON skal indeholde *use_ssl* key:
  ```
    {
        "use_ssl": false
    }
  ```

**SFTP:**
- **`kantinedata_sftp`**

  **Conn Type**: SFTP

  Bruges som `Connection id` i Airflow til at hente host, bruger, adgangskode og port til SFTP-forbindelsen

  *Required felter*:
  - Connection id, Host, Login, Password og Port (22)

## Schedule

Jobbet er sat op til at køre automatisk på følgende tidspunkter:

- **Schedule:** `@daily` (kører én gang dagligt)
- **Startdato:** 2026-04-08 (Europe/Copenhagen)
- **Catchup:** `false`

**Retry-policy**
- **Retries:** 1
- **Retry delay:** 12 timer