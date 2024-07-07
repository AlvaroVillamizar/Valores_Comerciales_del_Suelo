from Cleaning_Functions import DataProcessing
from Valor_del_Suelo_Catastro.Nearby_locations import run_nearby_locations

from sklearn.model_selection import KFold
import category_encoders as ce
import pandas as pd
import numpy as np
import random
import ast 

columns = ['longitud', 'latitud', 'precio', 'municipio', 'departamento', 'descripcion', 'titulo', 'tipo_inmueble', 'habitaciones', 'baños', 'estrato', 'area_total', 'area_construida', 'antigüedad', 'estado_inmueble', 'barrio', 'no_closet', 'tipo_registro']

def cleaning(n):
    rows = sum(1 for _ in open('Consulta_DNP-data.csv', 'r', encoding= 'utf8')) -1
    skip_indeces = sorted(random.sample(range(1, rows + 1), rows - n))

    df = pd.read_csv('Consulta_DNP-data.csv', skiprows = skip_indeces, usecols = columns, encoding='utf8')
    df['municipio'] = df['municipio'].fillna('')
    df['departamento'] = df['departamento'].fillna('')
    df['barrio'] = df['barrio'].fillna('')

    data_procesor = DataProcessing('Divipola.xlsx')
    
    df['departamento'] = df['departamento'].apply(lambda x: data_procesor.remove_accents(x))
    df['municipio'] = df['municipio'].apply(lambda x: data_procesor.remove_accents(x))
    df['barrio'] = df['barrio'].apply(lambda x: data_procesor.remove_accents(x))

    df['coordenadas'] = df['latitud'].astype(str) + ', '+ df['longitud'].astype(str)

    data_procesor.cleaning_antiguedad(df)
    data_procesor.fill_baños(df)
    data_procesor.fill_inmueble(df)
    data_procesor.fill_habitaciones(df)
    data_procesor.fill_estrato(df)
    data_procesor.fill_estado_inmueble(df)
    data_procesor.fill_registro(df)
    data_procesor.fill_no_closet(df)
    data_procesor.cleaning_municipio(df)
    data_procesor.metros_cuadrados(df)

    df = run_nearby_locations(df)
    df['log_precio'] = np.log(df['precio'])
    df['lugares_cercanos'] = df['lugares_cercanos'].apply(lambda x: len(ast.literal_eval(x)))
 
    kf = KFold(n_splits=5, shuffle=True, random_state=43)

    for train_index, val_index in kf.split(df):
        train_df, val_df = df.iloc[train_index], df.iloc[val_index]
        
        # Compute mean target for each category excluding the validation fold
        tipo_inmueble_mean = train_df.groupby('tipo_inmueble')['log_precio'].mean()
        estado_inmueble_mean = train_df.groupby('estado_inmueble')['log_precio'].mean()
        tipo_registro_mean = train_df.groupby('tipo_registro')['log_precio'].mean()
        
        # Map the means to the validation fold
        df.loc[val_index, 'tipo_inmueble_encoded'] = val_df['tipo_inmueble'].map(tipo_inmueble_mean)
        df.loc[val_index, 'estado_inmueble_encoded'] = val_df['estado_inmueble'].map(estado_inmueble_mean)
        df.loc[val_index, 'tipo_registro_encoded'] = val_df['tipo_registro'].map(tipo_registro_mean)

        df.drop(columns=['descripcion', 'titulo', 'tipo_registro', 'coordenadas', 'estado_inmueble', 'tipo_registro'])
        
    return df

if __name__ == "__main__":
    n = int(input("Enter the number of rows to load:"))
    df = cleaning(n)
    df.to_csv('cleaned_df_test2.csv', index= False)