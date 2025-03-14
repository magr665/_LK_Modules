"""
******* GIS-hjælpefunktioner *******
* Import dem med 'from LK_gis_helpers import addESRIGeom, ESRIclip'
* Tilføj geometri til en GeoDataFrame med 'addESRIGeom(df, drop_geom)'
* Klip en GeoDataFrame til en bounding box med 'ESRIclip(gdf, bbox)'

"""

import arcpy
import geopandas as gpd
from shapely.geometry import Polygon
import pandas as pd


def addESRIGeom(df, drop_geom = False):
    """
    Denne funktion, `addESRIGeom`, konverterer geometri fra en GeoDataFrame til et format, som kan bruges i ArcPy. 
    Funktionen tilføjer en ny kolonne `SHAPE@` til GeoDataFrame'en, som indeholder geometrien i ArcPy-format. 
    Der er også mulighed for at fjerne den oprindelige geometri-kolonne, hvis dette ønskes.

    Parametre:
    ----------
    - df: GeoDataFrame, der indeholder geospatiale data og en geometri-kolonne.
    - drop_geom: Boolsk værdi, der angiver, om den oprindelige geometri-kolonne skal fjernes. Standard er `False`.

    Returnerer:
    -----------
    - En kopi af den oprindelige GeoDataFrame med en tilføjet `SHAPE@`-kolonne indeholdende geometrien i ArcPy-format.

    Bemærkninger:
    -------------
    - Funktionen bruger `arcpy.FromWKT` til at konvertere geometri fra Well-Known Text (WKT) til ArcPy-format.
    - SpatialReference EPSG:25832 bruges som standard, hvilket svarer til UTM Zone 32N (ETRS89).
    """
    tmp_df = df.copy(deep=True)
    geometry_field_name = tmp_df.geometry.name
    tmp_df['temp_wkt_field'] = tmp_df[geometry_field_name].to_wkt()
    tmp_df['SHAPE@'] = tmp_df['temp_wkt_field'].apply(lambda geom: arcpy.FromWKT(geom, arcpy.SpatialReference(25832)))
    tmp_df.drop(columns=['temp_wkt_field'], inplace=True)
    if drop_geom == True:
        tmp_df.drop(columns=[geometry_field_name], inplace=True)
    return tmp_df        

def ESRIclip(gdf, bbox):
    """
    Klipper en GeoDataFrame til en specificeret bounding box (bbox).

    Denne funktion tager en GeoDataFrame (`gdf`) og klipper dens geometri til at være 
    inden for en angivet bounding box (`bbox`). Bounding box specificeres som en liste 
    med fire koordinater: [min_x, min_y, max_x, max_y].

    Parametre:
    ----------
    gdf : GeoDataFrame
        En GeoDataFrame, der indeholder geometri, som skal klippes.
    bbox : list
        En liste med fire koordinater [min_x, min_y, max_x, max_y], der definerer 
        den bounding box, som gdf skal klippes til.

    Returnerer:
    -----------
    GeoDataFrame
        En ny GeoDataFrame, som indeholder geometrierne fra `gdf`, der er inden for 
        den specificerede bounding box.
    """
    min_x = bbox[0]
    min_y = bbox[1]
    max_x = bbox[2]
    max_y = bbox[3]
    polygons = []
    corners = [(min_x, min_y), (min_x, max_y), (max_x, max_y), (max_x, min_y), (min_x, min_y)]
    
    polygon = Polygon(corners)
    polygons.append(polygon)
    
    # Opretter en polygon fra hjørnerne
    clip_gdf = gpd.GeoDataFrame(geometry=gpd.GeoSeries(polygons))
    clip_gdf.set_crs(epsg=25832, inplace=True)
    tmp_gdf = gpd.clip(gdf, clip_gdf)
    
    return tmp_gdf

def describeFC(sde, schema='*', feature_class='*'):
    desc_df = pd.DataFrame(columns=['Feature class', 'Feature type', 'Has M', 'Has Z', 'Coordinate system', 'Coordinate system name', 'Fields', 'Fields dict' 'Spatial Reference'])
    arcpy.env.workspace = sde
    fcs = arcpy.ListFeatureClasses(feature_class, 'ALL')
    if schema != '*':
        fcs = [fc for fc in fcs if fc.split('.')[0].lower() == schema.lower()]

    for fc in fcs:
        desc = arcpy.Describe(fc)
        sr = arcpy.SpatialReference(desc.spatialReference.factoryCode)
        flds = desc.fields
        fields = []
        fields_dict = {}
        for fld in flds:
            if fld.name.lower().startswith('shape') or fld.name.lower().startswith('objectid'):
                continue
            fldName = str(fld.name)
            fldType = str(fld.type)
            types = {
                'Blob': 'BLOB',
                'BigInteger': 'BIGINTEGER',
                'Date': 'DATE',
                'DateOnly': 'DATEONLY',
                'Double': 'DOUBLE',
                'Geometry': 'GEOMETRY',
                'GlobalID': 'GLOBALID',
                'Guid': 'GUID',
                'Integer': 'LONG',
                'OID': 'OID',
                'Raster': 'RASTER',
                'Single': 'FLOAT',
                'SmallInteger': 'SHORT',
                'String': 'TEXT',
                'TimeOnly': 'TIMEONLY',
                'TimeStampOffset': 'TIMESTAMPOFFSET'
            }

            fldLength = None
            if fldType.lower() == 'string':
                fldLength = fld.length
            
            if fldName == 'xTid':
                fldAlias = 'Sidst hentet'
            else:
                fldAlias = fld.aliasName
            fields.append([fldName, types[fldType], fldAlias, fldLength])
            fields_dict[fldName] = {'Type': types[fldType], 'Alias': fldAlias, 'Length': fldLength}

        new_row = pd.DataFrame([{
            'Feature class': fc,
            'Feature type': desc.shapeType,
            'Has M': desc.hasM,
            'Has Z': desc.hasZ,
            'Coordinate system': desc.spatialReference.factoryCode,
            'Coordinate system name': sr.name,
            'Fields': fields,
            'Fields dict': fields_dict,
            'Spatial Reference': sr
        }])
        desc_df = pd.concat([desc_df, new_row], ignore_index=True)
    return desc_df

if __name__ == "__main__":
    pass