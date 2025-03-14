"""
******* LK_unPack.py *******
* Importer den med 'from LK_unPack import unpack'
* Udpak en zip-fil med 'unpack(infile, outfolder, structure)'
* 'infile' er stien til zip-filen, der skal udpakkes
* 'outfolder' er stien til mappen, hvor filerne skal udpakkes
* 'structure' er en boolsk værdi, der angiver, om mappestrukturen skal bevares
* Returnerer ingen (None)
"""

import os
import shutil
from zipfile import ZipFile

def unpack(infile, outfolder, structure=False):
    """
    Udpakker en zip-fil til en angivet mappe med eller uden at bevare mappestrukturen.

    Denne funktion kan udpakke en zip-fil til en bestemt output-mappe. Hvis parameteren `structure` 
    er sat til `True`, vil den bevare den oprindelige mappestruktur fra zip-filen. Hvis `structure` 
    er `False`, vil alle filer blive udpakket direkte til output-mappen uden mappestrukturen.

    Parametre:
    ----------
    infile : str
        Stien til zip-filen, der skal udpakkes.
    outfolder : str
        Stien til mappen, hvor filerne skal udpakkes.
    structure : bool, valgfri
        Hvis `True`, bevares mappestrukturen fra zip-filen. Hvis `False`, udpakkes filerne uden struktur. 
        Standard er `False`.

    Returnerer:
    -----------
    Ingen (None).
    """
    if structure:
        with ZipFile(infile,'r') as zObject:
            zObject.extractall(path=outfolder)
    else:
        with ZipFile(infile) as zObject:
            for member in zObject.namelist():
                filename = os.path.basename(member)
                if not filename:
                    continue
                source = zObject.open(member)
                target = open(os.path.join(outfolder, filename), 'wb')
                with source, target:
                    shutil.copyfileobj(source, target)
    