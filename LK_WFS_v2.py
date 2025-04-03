"""
WFSClient klassen bruges til at kommunikere med WFS-tjenester.

Klassen håndterer automatisk:
- GetCapabilities forespørgsel for at hente metadata
- Opdeling af store datamængder i mindre bidder via bbox'e
- Konvertering af datofelter
- Klipning af data til bbox
- Authentication via brugernavn/kodeord

Parametre ved initialisering:
    url (str): URL til WFS-tjenesten
    username (str, optional): Brugernavn til authentication
    password (str, optional): Kodeord til authentication
    bbox (list, optional): Bounding box [minx, miny, maxx, maxy]
    debug (bool, optional): Debug mode, default False
    maxfeatures (int, optional): Max antal features per forespørgsel
    params (dict, optional): Ekstra parametre til WFS forespørgsler

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

class WFSClient:
    """
    WFSClient klassen bruges til at kommunikere med WFS-tjenester.

    Klassen håndterer automatisk:
    - GetCapabilities forespørgsel for at hente metadata 
    - Opdeling af store datamængder i mindre bidder via bbox'e
    - Konvertering af datofelter
    - Klipning af data til bbox
    - Authentication via brugernavn/kodeord

    Parametre ved initialisering:
        url (str): URL til WFS-tjenesten
        username (str, optional): Brugernavn til authentication
        password (str, optional): Kodeord til authentication  
        bbox (list, optional): Bounding box [minx, miny, maxx, maxy]
        debug (bool, optional): Debug mode, default False
        maxfeatures (int, optional): Max antal features per forespørgsel
        params (dict, optional): Ekstra parametre til WFS forespørgsler
    """
    def __init__(self, url: str, **kwargs):
        self.url = url
        for key, value in kwargs.items():
            setattr(self, key, value)

        ## check if debug is set
        if hasattr(self, 'debug'):
            self.__debug = self.debug
        else:
            self.__debug = False
            
        self.__params = {
            'service': 'WFS',
            'request': 'GetCapabilities'}
        
        if hasattr(self, 'params'):
            self.__params.update(self.params)     

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
        self.get_capabilities_root = root
        self.__namespace = self.get_capabilities_root.nsmap
        self.version = root.attrib['version']
        self.feature_list = self.__get_feature_list()
        self.operations = self.__get_operations()
        
        if 'GetFeature' in self.operations and 'parameters' in self.operations['GetFeature']:
            if 'resultType' in self.operations['GetFeature']['parameters'] and 'hits' in self.operations['GetFeature']['parameters']['resultType']:
                self.__can_get_hits = True
            else:
                self.__can_get_hits = False
        else:
            self.__can_get_hits = False

        if not hasattr(self, 'maxfeatures'):
            self.maxfeatures = self.__get_max_features()

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

    def __get_max_features(self):
        """
        Get the maximum number of features that can be returned by the WFS service.
        """
        constraint = self.get_capabilities_root.find('.//{*}Constraint[@name="CountDefault"]', namespaces=self.get_capabilities_root.nsmap)
        if constraint is not None:
            maxfeatures = constraint.find('.//{*}DefaultValue', namespaces=self.get_capabilities_root.nsmap)
            if maxfeatures is not None:
                return int(maxfeatures.text)
        print('No max features found, defaulting to 10000')
        return 10000

    def __get_feature_list(self):
        """
        Henter liste over feature lag fra WFS-tjenesten.

        Funktionen laver en liste over tilgængelige feature lag via GetCapabilities.
        Der hentes navn, titel, beskrivelse, koordinatsystem og bbox for hvert lag.

        Returnerer:
            dict: Dictionary med feature lag og deres metadata
        """
        feature_list = {}
        for feature_type in self.get_capabilities_root.findall(f'.//{{*}}FeatureType', namespaces=self.__namespace):
            feature = {}
            feature['name'] = feature_type.find(f'{{*}}Name', namespaces=self.__namespace)
            if feature['name'] is not None:
                feature['name'] = feature['name'].text
            feature['title'] = feature_type.find(f'{{*}}Title', namespaces=self.__namespace)
            if feature['title'] is not None:
                feature['title'] = feature['title'].text
            feature['abstract'] = feature_type.find(f'{{*}}Abstract', namespaces=self.__namespace)
            if feature['abstract'] is not None:
                feature['abstract'] = feature['abstract'].text
            feature['srs'] = feature_type.find(f'{{*}}DefaultCRS', namespaces=self.__namespace)
            if feature['srs'] is not None:
                feature['srs'] = feature['srs'].text
            feature['bbox'] = feature_type.find(f'{{*}}WGS84BoundingBox', namespaces=self.__namespace)
            if feature['bbox'] is not None:
                feature['bbox'] = [float(feature['bbox'].find(f'{{*}}LowerCorner', namespaces=self.__namespace).text.split()[0]),
                                   float(feature['bbox'].find(f'{{*}}LowerCorner', namespaces=self.__namespace).text.split()[1]),
                                   float(feature['bbox'].find(f'{{*}}UpperCorner', namespaces=self.__namespace).text.split()[0]),
                                   float(feature['bbox'].find(f'{{*}}UpperCorner', namespaces=self.__namespace).text.split()[1])]
            else:
                feature['bbox'] = feature_type.find(f'{{*}}LatLongBoundingBox', namespaces=self.__namespace)
                if feature['bbox'] is not None:
                    feature['bbox'] = [float(feature['bbox'].attrib['minx']),
                                       float(feature['bbox'].attrib['miny']),
                                       float(feature['bbox'].attrib['maxx']),
                                       float(feature['bbox'].attrib['maxy'])]
                else:
                    if feature['bbox'] is None:
                        try:
                            xMin = feature.find('.//{*}LowerCorner').text.split(' ')[0]
                            yMin = feature.find('.//{*}LowerCorner').text.split(' ')[1]
                            xMax = feature.find('.//{*}UpperCorner').text.split(' ')[0]
                            yMax = feature.find('.//{*}UpperCorner').text.split(' ')[1]
                            feature['bbox'] = [float(xMin), float(yMin), float(xMax), float(yMax)]
                        except:
                            feature['bbox'] = None
            feature_list[feature['name'].split(':')[-1]] = feature
            
        return feature_list

    def __get_operations(self):
        """
        Get the list of operations from the WFS service.
        """
        operations = {}
        if self.version >= '2.0.0':
            for operation in self.get_capabilities_root.findall(f'.//{{*}}Operation', namespaces=self.__namespace):
                op_name = operation.attrib['name']
                operations[op_name] = {}
                parameters = {}
                for parameter in operation.findall(f'{{*}}Parameter', namespaces=self.__namespace):
                    name = parameter.attrib['name']
                    # parameters[name] = {}
                    AllowedValues = parameter.find(f'{{*}}AllowedValues', namespaces=self.__namespace)
                    if AllowedValues is not None:
                        values = AllowedValues.findall(f'{{*}}Value', namespaces=self.__namespace)
                        if values is not None:
                            parameters[name] = [value.text for value in values]
                operations[op_name]['parameters'] = parameters
            return operations
        elif self.version < '2.0.0':
            for item in self.get_capabilities_root.findall(f'.//{{*}}Request', namespaces=self.__namespace):
                for req in item:
                    op_name = req.tag.split('}')[-1]
                    operations[op_name] = {}
            return operations
        else:
            return None                  
        
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
        if self.__debug: print('Clipped GeoDataFrame:')
        return gdf
    
    def __get_hits(self, feature_name, bbox):
        params = self.__params.copy()
        params.update({
            'request': 'GetFeature',
            'resulttype': 'hits',
            'version': self.version,
            'bbox': ','.join(bbox)
        })
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
        
    def __descripe_feature(self, feature_name):
        """
        Henter metadata for et specifikt feature lag fra WFS-tjenesten.
        
        Funktionen danner en WFS GetFeature forespørgsel med resulttype=describeFeature
        og returnerer metadata som en GeoDataFrame.
        
        Parametre:
            feature_name (str): Navnet på det ønskede feature lag
            
        Returnerer:
            GeoDataFrame: GeoDataFrame med metadata for det specifikke feature lag
            
        Raises:
            ValueError: Hvis GeoDataFrame ikke kan læses fra WFS-responsen
        """
        params = self.__params
        params['request'] = 'DescribeFeatureType'
        params['version'] = self.version
        if self.version in ('1.0.0', '1.1.0'):
            params['typename'] = feature_name
        else:
            params['typeNames'] = feature_name
        wfs_url = requests.Request('GET', self.url, params=params).prepare().url
        if self.__debug: 
            print('Getting DescribeFeatureType')
            print(wfs_url)

        response = requests.get(wfs_url)
        root = etree.XML(response.content)
        ns = root.nsmap
        ints = []
        decimals = []
        datetimes = []
        fc_schema = []
        for e in root.findall(f'.//{{*}}complexContent//{{*}}element', namespaces=ns):
            e = e.attrib
            dtype = e['type']
            # if self.__debug: print(f'Element: {e} - Type: {dtype}')
            if 'int' in dtype.lower():
                ints.append(e['name'])
            elif 'decimal' in dtype.lower():
                decimals.append(e['name'])
            elif 'date' in dtype.lower():
                datetimes.append(e['name'])
            else:
                fc_schema.append(e['name'])
        # if self.__debug: print(f'ints: {ints} - decimals: {decimals} - datetimes: {datetimes} - fc_schema: {fc_schema}')
        return {'ints':ints, 'decimals':decimals, 'datetimes':datetimes, 'fc_schema':fc_schema}

    def __get_feature(self, feature_name, bbox):
        params = self.__params.copy()
        params.update({
            'resulttype': 'results',
            'request': 'GetFeature',
            'bbox': ','.join(bbox),
            'version': self.version
        })
        if self.version in ('1.0.0', '1.1.0'):
            params['typeName'] = feature_name
        else:
            params['typeNames'] = feature_name
        
        wfs_url = requests.Request('GET', self.url, params=params).prepare().url
        if self.__debug: print('___get_features_gdf', wfs_url)
        try:
            return gpd.read_file(wfs_url, crs="EPSG:25832")
        except:
            # return gpd.read_file(wfs_url)
            raise ValueError('Could not read GeoDataFrame from WFS response')

    def get_features(self, feature_name, **kvargs):
        """
        Hent features fra et WFS lag.

        Funktionen laver en WFS GetFeature forespørgsel og returnerer data som GeoDataFrame.
        Den håndterer automatisk opdeling i mindre bbox'e, hvis der er for mange features.

        Parametre:
            feature_name (str): Navn på det ønskede WFS lag
            **kvargs: Ekstra parametre der tilføjes WFS forespørgslen
                
        Returnerer:
            GeoDataFrame: GeoDataFrame med features fra WFS laget
            
        Raises:
            ValueError: Hvis GeoDataFrame ikke kan læses fra WFS responsen
        """
        for key, value in kvargs.items():
            setattr(self, key, value)
        
        ## check if clip_gdf is set
        if hasattr(self, 'clip_gdf'):
            clip_gdf = self.clip_gdf
        else:
            clip_gdf = True

        if self.bboxes is None:
            self.bboxes = [[str(b) for b in self.feature_list[feature_name]['bbox']]]
            self.__default_bbox = self.bboxes[0]

        bboxes = self.bboxes.copy()
        if self.__debug: print(f'Bounding boxes: {self.bboxes}')
        gdfs = []
        for bbox in bboxes:
            if self.__debug: print(f'Bounding box: {bbox}')
            if self.__can_get_hits:
                hits = self.__get_hits(feature_name, bbox)
                if hits > self.maxfeatures:
                    if self.__debug: print(f'Number of features ({hits}) is greater than maxfeatures ({self.maxfeatures}), splitting bounding box')
                    for bb in self.__split_bbox(bbox):
                        bboxes.append(bb)
                else:
                    gdf = self.__get_feature(feature_name, bbox)
                    gdfs.append(gdf)
            else:
                gdf = self.__get_feature(feature_name, bbox)
                if len(gdf) > self.maxfeatures:
                    if self.__debug: print(f'Number of features ({len(gdf)}) is greater than maxfeatures ({self.maxfeatures}), splitting bounding box')
                    for bb in self.__split_bbox(bbox):
                        bboxes.append(bb)
                else:
                    gdfs.append(gdf)
        self.gdfs = gdfs
        gdf = pd.concat(gdfs, ignore_index=True)
        gdf.drop_duplicates(inplace=True)
        for col in gdf.columns.to_list():
            if '.' in col:
                gdf.rename(columns={col: col.replace('.', '_')}, inplace=True)

        gdf['xTid'] = pd.Timestamp.now()
        if clip_gdf and len(gdf) > 0:
            gdf = self.__clip_gdf(gdf)

        describe_feature = self.__descripe_feature(feature_name)
        if len(gdf) > 0:
            for col in gdf.columns.to_list():
                if col in describe_feature['datetimes']:
                    try:
                        gdf[col] = pd.to_datetime(gdf[col]).dt.tz_localize(None)
                    except Exception as e:
                        if self.__debug: print(f'Could not convert {col} to datetime: {e}')
                        try:
                            gdf[col] = pd.to_datetime(gdf[col], errors='coerce').dt.tz_localize(None)
                        except Exception as e:
                            if self.__debug: print(f'Could not convert {col} to datetime: {e}')
                            try:
                                gdf[col] = gdf[col].astype(str).str.replace('T', ' ').str.split('.').str[0]
                                gdf[col] = pd.to_datetime(gdf[col], format='%Y-%m-%d %H:%M:%S', errors='coerce').dt.tz_localize(None)
                            except Exception as e:
                                if self.__debug: print(f'Could not convert {col} to datetime: {e} at all')

        return gdf
