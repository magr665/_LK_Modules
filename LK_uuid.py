"""
******** LK_uuid.py ********
* Importer den med 'from LK_uuid import getUUID'
* Få en UUID med 'getUUID()'
* Returnerer en strengrepræsentation af en UUID

"""

import uuid
import os

def getUUID(uuidpath=None):
    """
    `getUUID`-funktionen genererer eller henter en unik UUID (Universally Unique Identifier) fra en fil kaldet `uuid.txt`, 
    som placeres i mappen over det aktuelle script. Hvis `uuid.txt` allerede eksisterer, læser funktionen UUID'en fra filen 
    og returnerer den. Hvis filen ikke eksisterer, opretter funktionen en ny UUID, gemmer den i filen og returnerer den.

    Funktionens proces:
    -------------------
    1. Bestemmer basePath for det aktuelle script.
    2. Beregner parentPath som mappen over basePath.
    3. Kontrollerer om filen `uuid.txt` eksisterer i parentPath:
    - Hvis filen eksisterer, læser og returnerer den UUID'en.
    - Hvis filen ikke eksisterer, genererer den en ny UUID, gemmer den i `uuid.txt`, og returnerer den.

    Returnerer:
    -----------
    - En strengrepræsentation af en UUID, enten hentet fra `uuid.txt` eller genereret og gemt i filen.

    Bemærkninger:
    -------------
    - Funktionen håndterer eventuelle undtagelser stille, dvs. uden at kaste fejl.
    - Hvis `uuid.txt` ikke kan findes eller oprettes, vil funktionen ikke returnere noget.
    """

    try:
        if uuidpath is None:
            return None
        uuid_file = os.path.join(uuidpath, 'uuid.txt')
        if os.path.exists(uuid_file):
            this_uuid = open(uuid_file, 'r').read()
            return this_uuid
        else:
            this_uuid = str(uuid.uuid4())
            with open(uuid_file, 'w') as f:
                f.write(this_uuid)
            return this_uuid
    except: 
        pass

if __name__ == "__main__":
    pass