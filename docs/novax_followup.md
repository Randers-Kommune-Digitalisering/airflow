# NOVAX followup

## Dato-vinduer

Formålet med dato-vinduerne er at fremsøge borgerne én gang hver måned op til termin, samt ~2 uger før termin:

- Én gang om måneden i samme “uge-blok” som deres terminsdato (uge 1 = d. 1–7, uge 2 = d. 8–14, uge 3 = d. 15–21, uge 4 = d. 22–28, uge 5 = månedens sidste 7 dage).
- Ca. 2 uger før terminsdato (14–20 dage før).

Funktionen beregner derfor et sæt dato-vinduer, som bruges til at filtrere i databasen:

1. Den tager udgangspunkt i “næste uge” (kørselsdato + 7 dage) og finder hvilken uge-blok i måneden den dato ligger i.
2. Den laver derefter månedlige vinduer for den samme uge-blok i den måned samt de næste måneder frem (standard 9 måneder).
3. Den tilføjer et ekstra vindue for terminsdatoer mellem 14 og 21 dage fra kørselsdatoen (start inklusiv, slut eksklusiv).
4. Hvis vinduer overlapper eller ligger lige op ad hinanden, bliver de lagt sammen til færre og større vinduer.
