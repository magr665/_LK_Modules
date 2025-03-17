"""
En WFS (Web Feature Service) klasse til at håndtere geografiske data fra webservices.

Denne klasse giver mulighed for at:
- Oprette forbindelse til en WFS-tjeneste
- Hente metadata om tilgængelige data
- Downloade geodata som GeoDataFrames
- Håndtere store datamængder gennem automatisk opdeling
- Klippe data til specificerede områder

Parametre:
    url (str): URL til WFS-tjenesten
    **kwargs: Valgfri parametre der kan inkludere:
        username (str): Brugernavn til autentificering
        password (str): Adgangskode til autentificering 
        bbox (list): Afgrænsningsboks [minx, miny, maxx, maxy]
        version (str): WFS version (default: nyeste tilgængelige)
        maxfeatures (int): Maks antal objekter der hentes
        debug (bool): Aktiver debug output
        outputFormat (str): Ønsket output format

Eksempel:
    >>> wfs = WFS('https://example.com/wfs', 
                  username='user',
                  password='pass',
                  bbox=[570000, 6200000, 580000, 6210000])
    >>> gdf = wfs.get_feature('kommuner')

Bemærk:
    Kræver geopandas, pandas, requests, lxml, shapely og fiona installeret
"""
import pandas as pd
import geopandas as gpd
import fiona
fiona.drvsupport.supported_drivers['WFS'] = 'r'
import requests
from xml.etree import ElementTree as ET
import lxml.etree as etree
from shapely.geometry import box


class WFS:
    """
    En klasse til at håndtere WFS (Web Feature Service) forespørgsler.

    Denne klasse giver mulighed for at interagere med WFS-tjenester, hente geometrier 
    og attributdata som GeoDataFrames.

    Parametre:
        url (str): URL'en til WFS-tjenesten
        **kwargs: Valgfri nøgleordsargumenter
        - username (str): Brugernavn til autentificering
        - password (str): Adgangskode til autentificering
        - bbox (list): Bounding box koordinater [minx, miny, maxx, maxy], hvis ikke angivet, findes bounding box i GetCapabilities responsen
        - version (str): WFS version, standard er nyeste understøttede, hvis ikke angiivet, findes version i GetCapabilities responsen
        - maxfeatures (int): Maks antal features der hentes, standard er 90% af MaxFeatures i GetCapabilities responsen
        - debug (bool): Aktiver debug output

    Attributter:
        - operations (dict): Tilgængelige WFS operationer
        - feature_types (dict): En dict med feature typer og antal features for den angivne bbox eller default bbox
        - bboxes (list): Liste over bounding boxes
        - maxfeatures (int): Maksimalt antal features der kan hentes
        - version (str): WFS version

    Metoder:
        - get_feature(feature_name): Henter features fra WFS-tjenesten som en GeoDataFrame
            - feature_name (str): Navnet på det ønskede feature lag
            - clip_gdf (bool): Hvis True, klippes GeoDataFrame til bounding box (standard er True)
    Raises:
        ValueError: Hvis påkrævede parametre mangler eller er ugyldige
    """
    def __init__(self, url: str, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
            # print(self, key, value)
        
        ## check if debug is set
        if hasattr(self, 'debug'):
            self.__debug = self.debug
        else:
            self.__debug = False

        ## initialize params
        self.url = url
        self.__params = {
            'service': 'WFS',
            'request': 'GetCapabilities',
        }
        
        ## check if username and password are provided together and add to params
        if (hasattr(self, 'username') and not hasattr(self, 'password')) or (hasattr(self, 'password') and not hasattr(self, 'username')):
            raise ValueError('Both username and password must be provided')

        if hasattr(self, 'username') and hasattr(self, 'password'):
            self.__params['username'] = self.username
            self.__params['password'] = self.password
        else:
            self.username = None
            self.password = None

        ## get capabilities
        url = requests.Request('GET', self.url, params=self.__params).prepare().url
        if self.__debug: print('GetCapabilities url:', url)
        response = requests.get(url)
        root = etree.XML(response.content)
        if self.__debug: 
            print('GetCapabilities response:', root)
        self.__get_capabilities_root = root
        
        ## find all operation names in root element
        self.operations = self.__get_operation_names()
        if self.__debug: print(self.operations)
        self.__get_maxfeatures()

        ## set default values
        if not hasattr(self, 'maxfeatures'):
            self.maxfeatures = self.operations['MaxFeatures'] * .98
        elif self.maxfeatures > self.operations['MaxFeatures']:
            self.maxfeatures = self.operations['MaxFeatures'] * .98
            print(f'MaxFeatures set to {self.maxfeatures}')

        if not hasattr(self, 'version'):
            self.version = sorted(self.operations['GetCapabilities']['AcceptVersions'], reverse=True)[0]
        elif self.version not in self.operations['GetCapabilities']['AcceptVersions']:
            raise ValueError(f'Version {self.version} not supported. Supported versions are {self.operations["GetCapabilities"]["AcceptVersions"]}')

        if hasattr(self, 'bbox'):
            if not isinstance(self.bbox, list):
                raise ValueError('bbox must be a list of coordinates [minx, miny, maxx, maxy]')
            if len(self.bbox) != 4:
                raise ValueError('bbox must be a list of coordinates [minx, miny, maxx, maxy]')
            self.bboxes = [[str(b) for b in self.bbox]]
            self.__default_bbox = self.bboxes[0]
            self.__missing_default_bbox = False
        else:
            self.bboxes = None
            self.__default_bbox = None
            self.__missing_default_bbox = True
    
        self.feature_types = self.__get_feature_types()

    def __get_bbox(self, typename):
        """
        Finder bounding box for et feature type fra WFS-tjenestens GetCapabilities respons.
        
        Metoden søger efter bounding box koordinater i GetCapabilities XML'en for det 
        angivne feature type. Hvis bounding box ikke kan findes, eller der opstår 
        en fejl, kastes en ValueError.
        
        Parametre:
            typename (str): Navnet på feature typen
            
        Returnerer:
            list: Liste med fire koordinater [minx, miny, maxx, maxy]
            
        Raises:
            ValueError: Hvis bounding box ikke kan findes eller hvis der opstår fejl
        """
        try:
            feature_item = self.__get_capabilities_root.find(f'.//{{*}}FeatureType[{{*}}Title="{typename}"]', namespaces=self.__get_capabilities_root.nsmap)
            if feature_item is not None:
                try:
                    xMin = feature_item.find('.//{*}LowerCorner').text.split(' ')[0]
                    yMin = feature_item.find('.//{*}LowerCorner').text.split(' ')[1]
                    xMax = feature_item.find('.//{*}UpperCorner').text.split(' ')[0]
                    yMax = feature_item.find('.//{*}UpperCorner').text.split(' ')[1]
                except:
                    xMin = feature_item.find('.//{{*}}LatLongBoundingBox', namespaces=self.__get_capabilities_root.nsmap).attrib['minx']
                    yMin = feature_item.find('.//{{*}}LatLongBoundingBox', namespaces=self.__get_capabilities_root.nsmap).attrib['miny']
                    xMax = feature_item.find('.//{{*}}LatLongBoundingBox', namespaces=self.__get_capabilities_root.nsmap).attrib['maxx']
                    yMax = feature_item.find('.//{{*}}LatLongBoundingBox', namespaces=self.__get_capabilities_root.nsmap).attrib['maxy']

                if any(coord is None for coord in [xMax, yMax, xMin, yMin]):
                    raise ValueError('Could not find bounding box in GetCapabilities response, please provide bbox as a parameter')

                gdf_bbox = box(xMin, yMin, xMax, yMax)

                # Opretter en GeoDataFrame med bounding box polygonen
                gdf = gpd.GeoDataFrame({'geometry': [gdf_bbox]})
                gdf.crs = "EPSG:4326"
                gdf = gdf.to_crs("EPSG:25832")
                self.__default_bbox = [str(xy) for xy in list(gdf.geometry[0].bounds)]
                return self.__default_bbox
            else:
                raise ValueError('Could not find bounding box in GetCapabilities response, please provide bbox as a parameter')
        except Exception as e: 
            if self.__debug: print(e)
            raise ValueError('Could not find bounding box in GetCapabilities response', e)

    def __get_operation_names(self):
        """
        Henter tilgængelige WFS operationer fra GetCapabilities responsen.
        
        Metoden parser GetCapabilities responsen og udtrækker alle operationer og deres 
        parametre. For hver operation gemmes de tilgængelige parameterværdier.
        
        Returnerer:
            dict: Dictionary med operationer som nøgler og deres parametre som værdier
            
        Eksempel på returværdi:
            {
            'GetCapabilities': {
                'AcceptVersions': ['2.0.0', '1.1.0', '1.0.0'],
                'AcceptFormats': ['text/xml']
            },
            'GetFeature': {
                'resultType': ['results', 'hits'],
                'outputFormat': ['application/json', 'text/xml']
            },
            'MaxFeatures': 10000
            }
        """
        operation_names = {}
        operation_elements = self.__get_capabilities_root.findall(f'.//{{*}}Operation', namespaces=self.__get_capabilities_root.nsmap)
        for element in operation_elements:
            try:
                operation_name  = element.attrib['name']
                if self.__debug: print(f'Getting operation: {operation_name}')
                parameters = {}
                for child in element:
                    vals = []
                    if f'Parameter' in child.tag:
                        param_name = child.attrib['name']
                        for val in child:
                            vals = [v.text for v in val]
                        parameters[param_name] = vals
                operation_names[operation_name] = parameters
            except Exception as e:
                if self.__debug: print(e)
                pass
        return operation_names
        
    def __get_maxfeatures(self):
        """
        Henter værdien for MaxFeatures fra GetCapabilities responsen.
        """
        try:
            constraint = self.__get_capabilities_root.find('.//{*}Constraint[@name="CountDefault"]', namespaces=self.__get_capabilities_root.nsmap)
            if self.__debug: print('Getting maxfeatures')
            if constraint is not None:
                default_value = constraint.find('.//{*}DefaultValue', namespaces=self.__get_capabilities_root.nsmap)
                if default_value is not None:
                    self.operations['MaxFeatures'] = int(default_value.text)
                else:
                    self.operations['MaxFeatures'] = 10000
            else:
                self.operations['MaxFeatures'] = 10000
        except Exception as e:
            if self.__debug: print(f"Error getting MaxFeatures: {e}")
            self.operations['MaxFeatures'] = 10000

    def __get_feature_types(self):
        """
        Henter en liste over tilgængelige feature typer fra WFS-tjenesten.
        
        Metoden parser GetCapabilities responsen og udtrækker navnene på alle 
        tilgængelige feature typer samt deres titler. Der oprettes en 
        oversættelsestabel mellem titler og tekniske navne.
        
        Returnerer:
            list: Liste over feature type titler

        """
        feature_types = {}
        feature_translations = {}
        feature_type_elements = self.__get_capabilities_root.findall(f'.//{{*}}FeatureType', namespaces=self.__get_capabilities_root.nsmap)
        for element in feature_type_elements:
            try:
                feature_name = element.find(f'.//{{*}}Name', namespaces=self.__get_capabilities_root.nsmap).text
                feature_title = element.find(f'.//{{*}}Title', namespaces=self.__get_capabilities_root.nsmap).text
                if self.__debug: print(f'Feature name: {feature_name}, Feature title: {feature_title}')

                feature_types[feature_title] = self.__get_hits(feature_name.split(':')[-1], self.__default_bbox, initial_hits=True)

                if feature_title is not None:
                    feature_translations[feature_title] = feature_name.split(':')[-1]
                else:
                    feature_translations[feature_name.split(':')[-1]] = feature_name.split(':')[-1]
            except Exception as e:
                if self.__debug: print(e)
                pass
        self.__feature_translations = feature_translations
        return feature_types
    

    def __split_bbox(self, bbox):
        """
        Opdeler en bounding box i to mindre bounding boxes.
        
        Funktionen tager en bounding box og opdeler den i to mindre bounding boxes
        langs den længste side. De nye bounding boxes tilføjes til self.bboxes.
        
        Parametre:
            bbox (list): Liste med fire koordinater [minx, miny, maxx, maxy]
            
        Tilføjer:
            De to nye bounding boxes til self.bboxes listen
        """
        minx = float(bbox[0])
        miny = float(bbox[1])
        maxx = float(bbox[2])
        maxy = float(bbox[3])

        if (maxx - minx) < (maxy - miny):
            bb1 = [str(minx), str(miny), str(maxx), str(miny + ((maxy - miny) / 2))]
            bb2 = [str(minx), str(miny + ((maxy - miny) / 2)), str(maxx), str(maxy)]
            return [bb1, bb2]
        else:
            bb1 = [str(minx), str(miny), str(minx + ((maxx - minx) / 2)), str(maxy)]
            bb2 = [str(minx + ((maxx - minx) / 2)), str(miny), str(maxx), str(maxy)]
            return [bb1, bb2]
        
    
    def __get_hits(self, feature_name, bbox, initial_hits=False):
        """
        Henter antallet af features fra WFS-tjenesten.
        
        Funktionen danner en WFS GetFeature forespørgsel med resulttype=hits
        for at få antallet af features indenfor den angivne bounding box.
        
        Parametre:
            feature_name (str): Navnet på det ønskede feature lag
            bbox (list): Bounding box koordinater [minx, miny, maxx, maxy]
            
        Returnerer:
            int: Antallet af features
            
        Raises:
            ValueError: Hvis antallet af features ikke kan læses fra WFS-responsen
        """
        params = {
            'version': self.version,
            'resulttype': 'hits',
            'service': 'WFS',
            'request': 'GetFeature',
            'username': self.username,
            'password': self.password,
        }
        if not initial_hits:
            params['bbox'] = ','.join(bbox)
        elif initial_hits and self.__missing_default_bbox is False:
            params['bbox'] = ','.join(self.__default_bbox)

        if self.version in ('1.0.0', '1.1.0'):
            params['typeName'] = feature_name
        else:
            params['typeNames'] = feature_name
        
        wfs_url = requests.Request('GET', self.url, params=params).prepare().url
        if self.__debug: print('hits url: ', wfs_url)
        response = requests.get(wfs_url)
        root = etree.XML(response.content)
        hits = int(root.attrib['numberMatched'])
        return hits
    

    def __get_features_gdf(self, feature_name, bbox, count= None):
        """
        Henter features fra WFS-tjenesten som en GeoDataFrame.
        
        Funktionen danner en WFS GetFeature forespørgsel med de angivne parametre og 
        returnerer resultatet som en GeoDataFrame.
        
        Parametre:
            feature_name (str): Navnet på det ønskede feature lag
            bbox (list): Bounding box koordinater [minx, miny, maxx, maxy]
            
        Returnerer:
            GeoDataFrame: GeoDataFrame med de hentede features
            
        Raises:
            ValueError: Hvis GeoDataFrame ikke kan læses fra WFS-responsen
        """
        params = self.__params
        params['version'] = self.version
        params['bbox'] = ','.join(bbox)
        params['resulttype'] = 'results'
        params['service'] = 'WFS'
        params['request'] = 'GetFeature'
        if self.version in ('1.0.0', '1.1.0'):
            params['typeName'] = feature_name
            if count is not None:
                params['maxfeatures'] = count
        else:
            params['typeNames'] = feature_name
            if count is not None:
                params['count'] = count
        if hasattr(self, 'outputFormat'):
            params['outputFormat'] = self.outputFormat
        wfs_url = requests.Request('GET', self.url, params=params).prepare().url
        if self.__debug: print('___get_features_gdf', wfs_url)
        try:
            return gpd.read_file(wfs_url, crs="EPSG:25832")
        except:
            # return gpd.read_file(wfs_url)
            raise ValueError('Could not read GeoDataFrame from WFS response')


    def __clip_gdf(self, tmp_gdf):
        """
        Klipper en GeoDataFrame til bounding box defineret i WFS-objektet.
        
        Parametre:
            gdf (GeoDataFrame): GeoDataFrame der skal klippes
            
        Returnerer:
            GeoDataFrame: Klippet GeoDataFrame
        """
        # tmp_gdf = gdf.copy()
        # tmp_gdf.crs = "EPSG:4326"
        # tmp_gdf = tmp_gdf.to_crs("EPSG:25832")
        gdf_bbox = box(float(self.__default_bbox[0]), float(self.__default_bbox[1]), float(self.__default_bbox[2]), float(self.__default_bbox[3]))
        gdf_bbox = gpd.GeoDataFrame({'geometry': [gdf_bbox]})
        gdf_bbox.crs = "EPSG:25832"
        try:
            tmp_gdf.set_crs("EPSG:25832", inplace=True)
            tmp_gdf = tmp_gdf.to_crs("EPSG:25832")
        except Exception as e:
            if self.__debug: print(e)
            pass
        if self.__debug:
            print('Clipping GeoDataFrame to bounding box')
            print(float(self.__default_bbox[0]), float(self.__default_bbox[1]), float(self.__default_bbox[2]), float(self.__default_bbox[3]))
            print(tmp_gdf.crs)
            print(gdf_bbox.crs)
        gdf = gpd.clip(tmp_gdf, gdf_bbox)
        return gdf


    def get_feature(self, feature_name, **kwargs):
        """
        Henter features fra WFS-tjenesten som en GeoDataFrame.
        
        Parametre:
            feature_name (str): Navnet på det ønskede feature lag
            **kwargs: Valgfri nøgleordsargumenter
            clip_gdf (bool): Hvis True, klippes GeoDataFrame til bounding box (standard: True)
            count (int): Antal features der skal hentes (standard: maxfeatures)
            
        Returnerer:
            GeoDataFrame: Pandas GeoDataFrame med de hentede features
            
        Eksempel:
            >>> wfs = WFS('https://example.com/wfs')
            >>> gdf = wfs.get_feature('kommuner')
        """

        for key, value in kwargs.items():
            setattr(self, key, value)
            # print(self, key, value)
        
        ## check if count is set
        if hasattr(self, 'count'):
            count = self.count

        ## check if clip_gdf is set
        if hasattr(self, 'clip_gdf'):
            clip_gdf = self.clip_gdf
        else:
            clip_gdf = True

        if self.bboxes is None:
            self.bboxes = [self.__get_bbox(feature_name)]
        if self.__debug: print(f'Bounding boxes: {self.bboxes}')
        feature_name = self.__feature_translations[feature_name]
        gdfs = []
        bboxes = self.bboxes.copy()
        if count is not None:
            gdf = self.__get_features_gdf(feature_name, bboxes[0], count)
        else:
            for bbox in bboxes:
                hits = self.__get_hits(feature_name, bbox)
                if hits > self.maxfeatures:
                    print(f'Number of hits {hits} exceeds maxfeatures {self.maxfeatures}. Splitting bbox')
                    for bb in self.__split_bbox(bbox):
                        bboxes.append(bb)
                else:
                    gdf = self.__get_features_gdf(feature_name, bbox)
                    gdfs.append(gdf)

            self.gdfs = gdfs
            gdf = pd.concat(gdfs, ignore_index=True)        
            gdf.drop_duplicates(inplace=True)
        for col in gdf.columns.to_list():
            if '.' in col:
                gdf.rename(columns={col: col.replace('.', '_')}, inplace=True)

        gdf['xTid'] = pd.Timestamp.now()

        if clip_gdf and self.__missing_default_bbox is False and len(gdf) > 0:
            gdf = self.__clip_gdf(gdf)
        return gdf        