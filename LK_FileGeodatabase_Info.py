"""
****** LK_FileGeodatabase_Info.py ******
* Importer den med 'from LK_FileGeodatabase_Info import gdb_info'
* Opret et gdb_info-objekt med 'gdb = gdb_info(gdb)'
* Få information om lagene i geodatabasen med 'gdb.info()'
* Få skemaet for et specifikt lag med 'gdb.schema(layernumber)'
* Konverter et lag til en GeoDataFrame med 'gdb.to_geodataframe(idx)'

"""

import os
import geopandas as gpd
import fiona

class gdb_info:
    """
    En klasse til at håndtere og udtrække information fra en File Geodatabase (GDB).
    Attributter:
    -----------
    gdb : str
        Stien til File Geodatabase.
    gdb_name : str
        Navnet på File Geodatabase.
    gdb_path : str
        Stien til mappen, der indeholder File Geodatabase.
    gdb_files : list
        En liste over lag i File Geodatabase med deres indeks og navne.
    Metoder:
    --------
    __init__(self, gdb):
        Initialiserer gdb_info objektet med stien til File Geodatabase.
    info(self, idx=None, fields=False):
        Returnerer information om lagene i File Geodatabase. Hvis 'idx' er specificeret, returneres information om det specifikke lag.
        Hvis 'fields' er True, inkluderes felterne i lagene.
    schema(self, layernumber):
        Returnerer skemaet for et specifikt lag i File Geodatabase.
    to_geodataframe(self, idx):
        Konverterer et specifikt lag i File Geodatabase til en GeoDataFrame.
    """
    def __init__(self, gdb):
        self.gdb = gdb
        self.gdb_name = os.path.basename(gdb)
        self.gdb_path = os.path.dirname(gdb)
        self.gdb_files = [(i, layername) for i, layername in enumerate(fiona.listlayers(gdb))]

    def info(self, idx=None, fields=False):
        """
        Henter information om lagene i en geodatabase.
        Args:
            idx (int, optional): Indeksen for det specifikke lag, der skal hentes information om. 
                                 Hvis None, hentes information om alle lag. Standard er None.
            fields (bool, optional): Hvis True, inkluderes felterne i lagets schema. Standard er False.
        Returns:
            dict: En ordbog med information om lagene. Hvis idx er None, returneres en ordbog med 
                  information om alle lag. Hvis idx er angivet, returneres en ordbog med information 
                  om det specifikke lag.
        """

        layers = {}
        if idx is None:
            for i, layername in enumerate(fiona.listlayers(self.gdb)):
                layer = {'Layername': layername}
                schema = self.schema(i)
                if schema['geometry'] == 'None':
                    layer['Type'] = 'Table'
                else:
                    layer['Type'] = 'Featureclass'
                    layer['Geometry'] = schema['geometry']

                if fields:
                    layer['Fields'] = schema['properties']
                layers[i] = layer
            return layers
        else:
            layer = {'Layername': fiona.listlayers(self.gdb)[idx]}
            schema = self.schema(idx)
            if schema['geometry'] == 'None':
                layer['Type'] = 'Table'
            else:
                layer['Type'] = 'Featureclass'
                layer['Geometry'] = schema['geometry']
            if fields:
                layer['Fields'] = schema['properties']
            return layer

    def schema(self, layernumber):
        """
        Henter skemaet for et bestemt lag i en geodatabase.
        Args:
            layernumber (int): Nummeret på laget i geodatabasen, som skemaet skal hentes for.
        Returns:
            dict: Et ordbog, der repræsenterer skemaet for det angivne lag.
        """

        schema = fiona.open(self.gdb, layer=layernumber).schema
        return schema
    
    def to_geodataframe(self, idx):
        """
        Konverterer en specifik lag fra en filgeodatabase til en GeoDataFrame.
        Parametre:
        idx (int): Indekset for laget i filgeodatabasen, der skal konverteres.
        Returnerer:
        GeoDataFrame: En GeoDataFrame, der repræsenterer det specificerede lag.
        """

        return gpd.read_file(self.gdb, layer=idx)