import numpy as np
import pandas as pd
import unicodedata
from fuzzywuzzy import fuzz, process

from geopy.geocoders import Nominatim
from geopy.distance import geodesic
from geopy.exc import GeocoderTimedOut
import time
import json
import ast
import re

# 
class DataProcessing:

    def __init__(self, filename):
        self.filename = filename
        self.geolocator = Nominatim(user_agent="BuscarCityDeptBarrio")

        self.divipola = pd.read_excel(open(self.filename, 'rb'), sheet_name='coordenadas')
        self.divipola['Nombre Departamento'] = self.divipola['Nombre Departamento'].apply(lambda x: self.remove_accents(x))
        self.divipola['Nombre Municipio'] = self.divipola['Nombre Municipio'].apply(lambda x: self.remove_accents(x))

        self.Mun_dict = self.Municipio_dict()
        self.Mun_dict['bogota'] = ['Bogota', 'Bogota d.c', 'Bogota, d.c', 'Bogota, d.c.', 'Bogota d.c.', 'Bogotá', 'Bogotá d.c', 'Bogotá, d.c', 'Bogotá, d.c.', 'Bogotá d.c.']

        self.sorted_keys = sorted(self.Mun_dict.keys(), key=len, reverse=True)
        self.Mun_dict = {key: self.Mun_dict[key] for key in self.sorted_keys}

        self.departament_list = [key for key in self.Mun_dict.keys()]
        self.municipios = {dep: self.divipola[self.divipola['Nombre Departamento'] == dep]['Nombre Municipio'].tolist() for dep in self.departament_list}
        self.municipios['bogota, d. c.'].append('bogota')

        self.municipio_pattern = re.compile(r'\b(?:{})\b'.format('|'.join(re.escape(mun) for mun_list in self.Mun_dict.values() for mun in mun_list)), re.IGNORECASE)
        self.departamento_pattern = re.compile(r'\b(?:{})\b'.format('|'.join(re.escape(dep) for dep in self.Mun_dict.keys())), re.IGNORECASE)

        
    @staticmethod
    def remove_accents(str):
        n_accents = unicodedata.normalize('NFKD', str)
        return ''.join([c for c in n_accents if not unicodedata.combining(c)]).lower()        

    def Municipio_dict(self):
        Departamentos = self.divipola['Nombre Departamento'].unique()
        Departamentos = np.append(Departamentos, ['Bogota'])

        Municipios_col = {}
        for index, row in self.divipola.iterrows():
            departamento = row['Nombre Departamento']
            municipio = row['Nombre Municipio']

            if departamento in Municipios_col:
                Municipios_col[departamento].append(municipio)

            else:
                Municipios_col[departamento] = [municipio]
        return Municipios_col

    def depto_barrio(self, coord, string):
        retries = 0
        while retries < 5:
            try:
                location = self.geolocator.reverse(coord, exactly_one=True)
                address = location.raw['address']
                if string == 'departamento':
                    dept = address.get('state', '')
                    dept = self.remove_accents(dept.lower())
                    
                    return dept

                elif string == 'barrio':
                    neighbourhood = address.get('neighbourhood', '')
                    neighbourhood = self.remove_accents(neighbourhood.lower())    
                
                    return neighbourhood
            
            except Exception as e:
                print(f'Error: {e}. Retrying ({retries+1}/{5})...')
                retries += 1
                time.sleep(1 * (2 ** retries))
                if retries == 5:
                    print(f'Failed to fetch data after {5} retries.')
                    break


    def mun_isna(self, text, coord, dept):
        if text == None:
            municipio = self.fill_municipio(coord, dept)
            return municipio
        else:
            return text
        
    def fill_municipio(self, coord, dept):
        retries = 0
        while retries < 5:
            try:
                location = self.geolocator.reverse(coord, exactly_one=True)
                address = location.raw['address']
                city = address.get('city', '')

                city = self.remove_accents(city.lower())
                city = self.find_municipio(city, dept)
                return city
                
            except Exception as e:
                print(f'Error: {e}. Retrying ({retries+1}/{5})...')
                retries += 1
                time.sleep(1 * (2 ** retries))
                if retries == 5:
                    print(f'Failed to fetch data after {5} retries.')
                    break

    def find_departamento(self, text):
        for dept in self.departament_list:
            if dept in text:
                return dept
        municipio = self.municipio_pattern.search(str(text))
        if municipio:
            municipio = municipio.group()
        if municipio in self.divipola['Nombre Municipio'].tolist():
            dataset = self.divipola[self.divipola['Nombre Municipio'] == municipio]
            if dataset.shape[0] == 1:
                return dataset['Nombre Departamento'].item()
            
    def find_bogota(self, text):
        match = re.search(r'bogota', text)
        if match:
            return True
        else:
            return False

    def find_municipio(self, text, departamento):
        new_text = text.replace(departamento, '') # string without departamento in it 
        municipios_en_departamento = self.municipios[departamento] # list of municipios in the found departamento
        
        while new_text:
            found_municipio = self.municipio_pattern.search(str(new_text))
            if found_municipio:
                municipio = found_municipio.group()
            else:
                municipio = new_text
            matches =  process.extract(municipio, municipios_en_departamento, scorer=fuzz.token_sort_ratio, limit=1)
            if matches:
                best_match= matches[0]
                if best_match[1] > 80:
                    return best_match[0]
                else:
                    new_text = new_text.replace(municipio, '').strip()
            else:
                break
        return None
    
    def dictionary_codes(self):
        divipola = self.divipola[['Nombre', 'Divipola_mun', 'Nombre Departamento', 'Divipola_dep']]

        Municipios_translate = {}
        Departamento_translate = {}

        for _, row in divipola.iterrows():
            mun = row['Nombre']
            mun_cod = row['Divipola_mun']
            dep = row['Nombre Departamento']
            dep_cod = row['Divipola_dep']
            
            if mun not in Municipios_translate:
                Municipios_translate[mun] = mun_cod
            if dep not in Departamento_translate:
                Departamento_translate[dep] = dep_cod
                
        return Municipios_translate, Departamento_translate

    def cleaning_municipio(self, df):
        for index, row in df.iterrows():
            text = row['municipio']
            found_bogota = self.find_bogota(text)
            
            if found_bogota:
                
                df.at[index, 'municipio'] = 'bogota, d. c.'
                df.at[index, 'departamento'] = 'bogota, d. c.'
                
            else:   
                departamento = row['departamento']
                if departamento == '':  
                    departamento = self.find_departamento(text)
                    municipio = self.find_municipio(text, departamento)
        
                    municipio = self.mun_isna(municipio, row['coordenadas'], departamento)
                    df.at[index, 'municipio'] = municipio
                    df.at[index, 'departamento'] = departamento
        
                elif self.municipios.get(departamento): #Caso en el que departamento sea incorrecto
                    municipio = self.find_municipio(text, departamento)
        
                    municipio = self.mun_isna(municipio, row['coordenadas'], departamento)
                    df.at[index, 'municipio'] = municipio
                    df.at[index, 'departamento'] = departamento
                else:
                    matches =  process.extract(departamento, self.municipios.keys(), scorer=fuzz.token_sort_ratio, limit=1)
                    if matches:
                        best_match= matches[0]
                        if best_match[1] > 80:
                            departamento = best_match[0]
                        if self.municipios.get(departamento): 
                            municipio = self.find_municipio(text, departamento)
                            
                            municipio = self.mun_isna(municipio, row['coordenadas'], departamento)
                            df.at[index, 'municipio'] = municipio
                            df.at[index, 'departamento'] = departamento

            barrio = row['barrio'] 
            if barrio == '':
                barrio = self.depto_barrio(row['coordenadas'], 'barrio')
                df.at[index, 'barrio'] = barrio 

    def cleaning_antiguedad(self, df):
        for index, row in df.iterrows():
            numbers = re.findall(r'\d+', str(row['antigüedad']))
            if len(numbers) == 1:  
                df.at[index, 'antigüedad'] = numbers[0]
            elif len(numbers) > 1:  
                number = (int(numbers[0]) + int(numbers[-1])) / 2
                number =  round(number,2)
                df.at[index, 'antigüedad'] = number
            else:
                df.at[index, 'antigüedad'] = np.nan

    def fill_area(self, row_header, pattern, df, row, index):
        match = re.findall(pattern, row[row_header], re.IGNORECASE)
        if match:
            values = []
            for item in match:
                values.append(float(item[0]))
            df.at[index, 'area_construida'] = max(values)
        return match

    def metros_cuadrados(self, df):
        for index, row in df.iterrows():
            area_total = row['area_total']
            area_cons = row['area_construida']
            
            if (not pd.isna(area_total)) and (not pd.isna(area_cons)):
                if area_cons > area_total:
                    df.at[index, 'area_total'] = area_cons
                    df.at[index, 'area_construida'] = area_total
            
            pattern = r'(\d+)\s*(m2|mt2|ms2|mts2)'    
            if pd.isna(area_cons): 
                match = self.fill_area('descripcion', pattern, df, row, index)
                if match and pd.isna(area_total):
                    df.at[index, 'area_total'] = df.at[index, 'area_construida']
                elif not match:
                    match = self.fill_area('titulo', pattern, df, row, index)
                    if not pd.isna(area_total):
                            df.at[index, 'area_construida'] = df.at[index, 'area_total'] 
            
            if pd.isna(area_total):
                df.at[index, 'area_total'] = df.at[index, 'area_construida'] 

    def fill_baños(self, df):
        for index, row in df.iterrows():
            pattern = r'(\d+)\s*(baño|ba‚àö¬±o)'

            if pd.isna(row['baños']):
                match = re.findall(pattern, row['descripcion'], re.IGNORECASE)
                if match:
                    values = []
                    for item in match:
                        values.append(int(item[0]))
                    df.at[index, 'baños'] = max(values)
            if pd.isna(row['baños']):
                pattern = r'(baño|ba‚àö¬±o)*(\d+)\s'


    def fill_inmueble(self, df):
        inmuebles = list(df[~df['tipo_inmueble'].isna()]['tipo_inmueble'].unique())
        inmuebles = sorted(inmuebles, key=len, reverse=True)
        inmuebles.append('apto')
        inmueble_pattern = re.compile(r'\b(?:{})\b'.format('|'.join(re.escape(inmueble) for inmueble in inmuebles)), re.IGNORECASE)
        for index, row in df.iterrows():
            inmueble = row['tipo_inmueble']
            if pd.isna(inmueble):
                new_inmueble = inmueble_pattern.search(str(row['descripcion']))
                if new_inmueble:
                    if new_inmueble.group().lower() == 'apto':
                        df.at[index, 'tipo_inmueble'] = 'apartamento'
                    else:
                        df.at[index, 'tipo_inmueble'] = new_inmueble.group().lower()
                else:
                    new_inmueble = inmueble_pattern.search(str(row['titulo']))
                    if new_inmueble:
                        if new_inmueble.group().lower() == 'apto':
                            df.at[index, 'tipo_inmueble'] = 'apartamento'
                        else:
                            df.at[index, 'tipo_inmueble'] = new_inmueble.group().lower()


            new_inmueble = inmueble_pattern.search(str(row['descripcion']))
            if new_inmueble:
                if inmueble != new_inmueble:
                    if new_inmueble.group().lower() == 'apto':
                        df.at[index, 'tipo_inmueble'] = 'apartamento'
                    else:
                        df.at[index, 'tipo_inmueble'] = new_inmueble.group().lower()
            else:
                new_inmueble = inmueble_pattern.search(str(row['titulo']))
                if new_inmueble: 
                    if inmueble != new_inmueble:
                        if new_inmueble.group().lower() == 'apto':
                            df.at[index, 'tipo_inmueble'] = 'apartamento'
                        else:
                            df.at[index, 'tipo_inmueble'] = new_inmueble.group().lower()

    def fill_habitaciones(self, df):
        pattern = re.compile(r'(\d+)\s*(habitacion|alcoba)', re.IGNORECASE)
        for index, row in df.iterrows():
            habitaciones = row['habitaciones']
            if pd.isna(habitaciones):
                new_habitacion = pattern.search(str(row['descripcion']))
                if new_habitacion:
                    df.at[index, 'habitaciones'] = int(new_habitacion.group(1))
                else:
                    new_habitacion = pattern.search(str(row['titulo']))
                    if new_habitacion:
                        df.at[index, 'habitaciones'] = int(new_habitacion.group(1))

    def fill_estrato(self, df):
        pattern = re.compile(r'(\d+)\s*(estrato)', re.IGNORECASE)
        for index, row in df.iterrows():
            estrato = row['estrato']
            if pd.isna(estrato):
                new_estrato = pattern.search(str(row['descripcion']))
                if new_estrato:
                    df.at[index, 'estrato'] = int(new_estrato.group(1))
                else:
                    new_estrato = pattern.search(str(row['titulo']))
                    if new_estrato:
                        df.at[index, 'estrato'] = int(new_estrato.group(1))

    def fill_estado_inmueble(self, df):
        for index, row in df.iterrows():
            if isinstance(row['estado_inmueble'], str) and row['estado_inmueble'].startswith('{'):
                try:
                    new_estado_inmueble = json.loads(row['estado_inmueble'].replace("'", "\""))['name']
                    df.at[index, 'estado_inmueble'] = new_estado_inmueble
                except json.JSONDecodeError:
                    None

    def fill_registro(self, df):
        for index, row in df.iterrows():
            if pd.isna(row['tipo_registro']):
                pattern = r'vent|vend|arriend'
                match = re.search(pattern, row['descripcion'], re.IGNORECASE)
                if match:
                    if (match.group(0).lower() == 'vend') or (match.group(0).lower() == 'vent'):
                        df.at[index, 'tipo_registro'] = 'venta'
                    elif (match.group(0).lower() == 'arriend'):
                        df.at[index, 'tipo_registro'] = 'arriendo'
                else:
                    pattern = r'vent|vend|arriend'
                    match = re.search(pattern, row['titulo'], re.IGNORECASE)
                    if match:
                        if (match.group(0).lower() == 'vend') or (match.group(0).lower() == 'vent'):
                            df.at[index, 'tipo_registro'] = 'venta'
                        elif (match.group(0).lower() == 'arriend'):
                            df.at[index, 'tipo_registro'] = 'Arriendo'
            elif df.at[index, 'tipo_registro'][-5:].lower() == 'venta':
                    df.at[index, 'tipo_registro'] = 'venta'

    def fill_no_closet(self, df):
        for index, row in df.iterrows():
            closet = row['no_closet']
            if isinstance(closet, str) and 'no_closet:' in closet:
                match = re.search(r'no_closet:(.*)', closet)
                if match: 
                    df.at[index, 'no_closet'] = match.group(1)
            
            elif pd.isna(closet):
                df.at[index, 'no_closet'] = ''