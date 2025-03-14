# PythonScripts Udvikling

Velkommen til mine python scripts, som hjælper mig i hverdagen så jeg ikke skal sidde og skrive de samme ting igen og igen.

## Indhold

- [Introduktion](#introduktion)
- [Installation](#installation)
- [Brug](#brug)
- [Bidrag](#bidrag)
- [Licens](#licens)

## Introduktion

Dette projekt indeholder følgende Python-scripts:

- `LK_boundingBox.py`: Indeholder klassen `BoundingBox`, som håndterer geografiske bounding boxes, herunder opdeling og arealberegning.
- `LK_DatabaseConnections.py`: Håndterer databaseforbindelser og indeholder funktioner til at oprette og administrere disse forbindelser.
- `LK_emailer.py`: Indeholder funktioner til at sende e-mails ved hjælp af forskellige e-mail-tjenester.
- `LK_FileGeodatabase_info.py`: Indeholder funktioner som returnere en pandas dataframe med informationer om en fil-geodatabase
- `LK_gis_helpers.py`: Indeholder hjælpefunktioner til geografiske informationssystemer (GIS).
- `LK_LKuuid.py`: Indeholder funktionen `getUUID`, som genererer eller henter en unik UUID fra en fil.
- `LK_logger.py`: Håndterer logning af beskeder og fejl i projektet.
- `LK_unPack.py`: Indeholder funktioner til at pakke og udpakke filer og mapper.

## Installation

For at installere dette projekt, skal du klone repositoryet og installere de nødvendige afhængigheder:

```bash
git clone <repository-url>
cd PythonScripts
pip install -r requirements.txt
```

## Brug

For at bruge et af modulerne, kan du importere det i din Python-kode:

```python
import modulnavn
```

Se den medfølgende dokumentation for hvert modul for specifikke brugsanvisninger.

## Bidrag

Vi byder bidrag velkommen! Følg venligst disse trin for at bidrage:

1. Fork repositoryet
2. Opret en ny branch (`git checkout -b feature/dit-feature-navn`)
3. Commit dine ændringer (`git commit -m 'Tilføj feature'`)
4. Push til branchen (`git push origin feature/dit-feature-navn`)
5. Opret en Pull Request

## Licens

Dette projekt er licenseret under MIT-licensen. Se filen `LICENSE` for flere detaljer.