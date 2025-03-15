# PythonScripts Udvikling

Velkommen til mine python scripts, som hjælper mig i hverdagen så jeg ikke skal sidde og skrive de samme ting igen og igen.

## Indhold

- [Introduktion](#introduktion)
- [Installation](#installation)
- [Brug](#brug)
- [Bidrag](#bidrag)
- [Licens](#licens)

## Introduktion
Dette er en samling af Python-moduler, som jeg har udviklet for at automatisere og effektivisere mine daglige arbejdsopgaver. Modulerne tilbyder funktionalitet til alt fra logning og databaseforbindelser til email-afsendelse og GIS-databehandling.

Hver modul er designet til at løse specifikke problemer, jeg ofte støder på, og gør det muligt at genbruge kode på tværs af forskellige projekter. Denne modulsamling sparer mig for tid og sikrer konsistente resultater i mine scripts.

Modulerne kan bruges individuelt eller i kombination for at bygge mere komplekse applikationer og databehandlingsflows.

## Installation

For at installere dette projekt, skal du klone repositoryet og installere de nødvendige afhængigheder:

```bash
git clone <repository-url>
cd PythonScripts
pip install -r requirements.txt
```
Der kan være flere requirements som ikke lige er nævnt, men hvis det er tilfældet, så hører jeg gerne om det, så jeg kan rette filen.

## Brug

For at bruge et af modulerne, kan du importere det i din Python-kode f.eks.:

```python
from LK_logger import Logger
```

Se den medfølgende dokumentation for hvert modul for specifikke brugsanvisninger.

## Moduler

### LK_boundingBox
* Er blevet en integreret del af LK_WFS

### LK_DatabaseConnections
* Opretter en connection, engine og cursor til en database

### LK_emailer
* Sender en mail, evt. med en vedhæftet fil, kan håndtere html eller alm. tekst

### LK_FileGeodatabase_Info
* Finder info om en GeoDatabase, alle featureclasses, deres fields m.m.
* Kan muligvis også bruges på en SDE connection, er ikke helt testet færdigt.

### LK_gis_helpers

### LK_logger
* Bruges til at logge forskellige steps i ens scripts.
* Formatet er:
__2025-03-11 09:15:53 : INFO     : Starting__

* Input kan være info, warning eller critical, kan også tjekke om der er warning eller critical i ens log

### LK_unPack
* Bruges til at unpacke zip-filer, kan overholde mappestrukturen i filen eller ej

### LK_uuid
* Opretter en fil med et uuid, bruges når der køres statestik med LK_DatabaseConnections

### LK_WFS
* Bruges til at hente WFS data, svaret er en GeoPandas Dataframe



## Bidrag

Vi byder bidrag velkommen! Følg venligst disse trin for at bidrage:

1. Fork repositoryet
2. Opret en ny branch (`git checkout -b feature/dit-feature-navn`)
3. Commit dine ændringer (`git commit -m 'Tilføj feature'`)
4. Push til branchen (`git push origin feature/dit-feature-navn`)
5. Opret en Pull Request

## Licens

Dette projekt er licenseret under MIT-licensen. Se filen `LICENSE` for flere detaljer.