import pandas as pd
import os

data_path = './cicese'

# For para iterar cada ciudad
for city in os.listdir(data_path):
    variables_path = f"{data_path}/{city}" # Se arma el path hacia la ciudad
    # For para iterar las variables de la ciudad
    dfs_list = [] # Lista para guardar los dataframes de cada variable
    for var in os.listdir(variables_path):
        print(var)
        current_var_path = f"{variables_path}/{var}" # Se arma el path hacia la variable
        # For para iterar los csv's de las diferentes estaciones de la ciudad para la variable en cuestión
        variable_df = pd.DataFrame()
        for file in [f for f in os.listdir(current_var_path) if os.path.isfile(f"{current_var_path}/{f}")]:
            file_path = f"{current_var_path}/{file}" # Se arma el path del csv
            file = open(file_path) # Se abre el archivo
            id_estacion = file.readline().split(',')[1].strip() # Se lee el id de la estación
            nombre_estacion = file.readline().split(',')[1].strip() # Se lee el nombre de la estación
            coordenadas_geograficas_estacion = file.readline().split(',')[1].strip() # Se leen las coordenadas geográficas de la estación
            file.close() # Se cierra el archivo
            
            # Se lee como csv todo después de la cabecera del archivo. Al mismo tiempo se forma la fecha con las
            # primeras 3 columnas.
            current_df = pd.read_csv(file_path, skiprows=7, parse_dates={'Fecha':[0,1,2]})
            # Se cambia de nombre la columna "Datos" por el nombre de la variable en cuestión
            current_df.rename(columns={'Datos': var}, inplace=True)
            # Se añade el id, nombre y coordenadas geográficas de la estación. Estos datos se obtuvieron
            # del encabezado.
            current_df['IdEstacion'] = id_estacion
            current_df['NombreEstacion'] = nombre_estacion
            current_df['CoordGeoEstacion'] = posicion_estacion
            #print(current_df.tail())
            # Se concatena el dataframe de este archivo con el dataframe que contiene los datos totales de la variable en
            # cuestión.
            variable_df = pd.concat([variable_df, current_df])
        
        # El dataframe completo de la variable en cuestión se añade a la lista de dataframes
        print(len(variable_df))
        dfs_list.append(variable_df)
    
    # Una vez que todos los dataframes de todas las variables se han armado, ahora se van a unir entre sí para que 
    # todas las variables aparezcan en un mismo dataframe. Se tomará como llave la Fecha y para almacenar toda
    # la información de cada variable se realizará un outer o full join.
        
    full_df = dfs_list.pop() # Se toma un df para comenzar
    # Mientras existan dataframes se va a realizar un full join con el dataframe total
    while dfs_list:
        full_df = pd.merge(full_df, dfs_list.pop(), on=['Fecha', 'IdEstacion', 'NombreEstacion', 'CoordGeoEstacion'], how='outer')
    
    # Se reordenan las columnas
    ordered_columns = ['Fecha', 'IdEstacion', 'NombreEstacion', 'CoordGeoEstacion', 'tmin', 'tmax', 'tprom', 'unidadesCalor', 'evaporacion', 'precipitacion']
    full_df = full_df[ordered_columns]
    # Se exporta el resultado a un csv
    full_df.to_csv('hermosillo.csv', index=False)
            