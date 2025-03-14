"""
************ Bounding Box ************
* Importer den med `from LK_boundingBox import BoundingBox`
* Opret et BoundingBox-objekt med `bbox = BoundingBox(defaultBox)`
* Få standard bounding box med `bbox.defaultBBOX()`
* Opdel en bounding box med `bbox.splitBbox(box)`
* Beregn arealet af en bounding box med `bbox.getArea(bbox, output)`

"""

import geopandas as gpd
from shapely.geometry import box

class BoundingBox:
    """
    En klasse til at håndtere geografiske bounding boxes (afgrænsningsbokse).

    Klassen BoundingBox giver funktionalitet til at opbevare, opdele og beregne arealet af en bounding box. 
    Den kan returnere arealet i forskellige enheder som kvadratmeter, hektar og kvadratkilometer.

    Attributter:
    -----------
    initBBOX : list
        Standard bounding box, der er angivet ved initialiseringen.
    """

    def __init__(self, defaultBox):
        """
        Initialiserer BoundingBox-objektet med en standard bounding box.

        Parametre:
        ----------
        defaultBox : list
            En liste med fire værdier, der repræsenterer en standard bounding box i formatet [minx, miny, maxx, maxy].
        """
        self.initBBOX = defaultBox

    def defaultBBOX(self):
        """
        Returnerer den standard bounding box, der blev angivet ved initialiseringen.

        Returnerer:
        -----------
        list
            Standard bounding box som en liste [minx, miny, maxx, maxy].
        """
        return self.initBBOX
    
    def splitBbox(self, box):
        """
        Opdeler en bounding box i to mindre bokse afhængigt af dens proportioner.

        Hvis bounding boxen er højere end den er bred, opdeles den horisontalt.
        Hvis den er bredere end den er høj, opdeles den vertikalt.

        Parametre:
        ----------
        box : list
            En liste med fire værdier, der repræsenterer en bounding box i formatet [minx, miny, maxx, maxy].

        Returnerer:
        -----------
        list
            En liste med to lister, hver repræsenterende en mindre bounding box.
        """
        bboxes = []
        minx = float(box[0])
        miny = float(box[1])
        maxx = float(box[2])
        maxy = float(box[3])

        if (maxx - minx) < (maxy - miny):
            bb1 = [str(minx), str(miny), str(maxx), str(miny + ((maxy - miny) / 2))]
            bb2 = [str(minx), str(miny + ((maxy - miny) / 2)), str(maxx), str(maxy)]
            bboxes.append(bb1)
            bboxes.append(bb2)
        else:
            bb1 = [str(minx), str(miny), str(minx + ((maxx - minx) / 2)), str(maxy)]
            bb2 = [str(minx + ((maxx - minx) / 2)), str(miny), str(maxx), str(maxy)]
            bboxes.append(bb1)
            bboxes.append(bb2)
        
        return bboxes
    
    def getArea(self, bbox, output='m2'):
        """
        Beregner arealet af en bounding box og returnerer det i den ønskede enhed.

        Parametre:
        ----------
        bbox : list
            En liste med fire værdier, der repræsenterer en bounding box i formatet [minx, miny, maxx, maxy].
        output : str, valgfri
            Enheden for arealet, som enten kan være 'm2' (kvadratmeter), 'ha' (hektar) eller 'km2' (kvadratkilometer). 
            Standard er 'm2'.

        Returnerer:
        -----------
        float
            Arealet af bounding boxen i den valgte enhed.
        """
        minx = float(bbox[0])
        miny = float(bbox[1])
        maxx = float(bbox[2])
        maxy = float(bbox[3])

        bbox = box(minx, miny, maxx, maxy)

        # Opretter en GeoDataFrame med bounding box polygonen
        gdf = gpd.GeoDataFrame({'geometry': [bbox]}, crs="EPSG:25832")  # Antager at koordinaterne er i WGS 84

        # Beregner arealet i kvadratmeter
        area_m2 = round(gdf['geometry'].area[0], 2)

        # Konverterer til kvadratkilometer
        area_km2 = round(area_m2 / 1_000_000, 2)

        # Konverterer til hektar
        area_ha = round(area_m2 / 10_000, 2)

        if output == 'km2':
            return area_km2
        elif output == 'ha':
            return area_ha
        else:
            return area_m2
