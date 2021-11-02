import pandas as pd
import os
import shutil
import zipfile
from zipfile import ZipFile
# Ruta a la carpeta de datos
data_path = './datos'
zip_file = f"{data_path}/clicom_mex_cicese_sonora.zip"
data_folder = f"{data_path}/clicom_mex_cicese_sonora"


def readCsvAsDataFrame(file_path):
    """
        Esta función lee un csv al estilo de CICESE y cambia su formato
        de acuerdo a las necesidades de este proyecto
        
        Parámetros
        ----------
        file_path: Ruta hacia el csv que se desea leer
        
        Regresa
        -------
        current_df: Dataframe que contiene la información del csv
                    pero con el formato corregido
    """
    file = open(file_path) # Se abre el archivo
    id_estacion = file.readline().split(',')[1].strip() # Se lee el id de la estación
    nombre_estacion = file.readline().split(',')[1].strip() # Se lee el nombre de la estación
    coordenadas_geograficas_estacion = file.readline().split(',')[1].strip() # Se leen las coordenadas geográficas de la estación
    variable = file.readline().split(',')[1].strip() # Se leen la variable que contiene el archivo
    file.close() # Se cierra el archivo

    # Se leen como csv todos los renglones después de la cabecera del archivo. Al mismo tiempo se forma la fecha con las
    # primeras 3 columnas.
    current_df = pd.read_csv(file_path, skiprows=7, parse_dates={'Fecha':[0,1,2]})
    # Se cambia de nombre la columna "Datos" por el nombre de la variable en cuestión
    current_df.rename(columns={'Datos': variable}, inplace=True)
    # Se añade el id, nombre y coordenadas geográficas de la estación. Estos datos se obtuvieron
    # del encabezado.
    current_df['IdEstacion'] = id_estacion
    current_df['NombreEstacion'] = nombre_estacion
    current_df['CoordGeoEstacion'] = coordenadas_geograficas_estacion
    
    return current_df

# Se extraen los datos del .zip
with ZipFile(zip_file, 'r') as zip:
    zip.extractall(data_path)

# Lista de archivos
archivos = os.listdir(data_folder)
# Se intenta remover los checkpoints de jupyter
try:
    archivos.remove('.ipynb_checkpoints')
except:
    None

# En este diccionario se van a guardar los dataframes leídos ordenados por variable para evitar duplicar columnas al momento de
# ejecutar el outer join
dfs_dict = {'Evaporación (mm)':[], 'Precipitación (mm)':[], 'Temp Máxima (oC)':[], 'Temp Mínima (oC)':[], 'Temp Promedio (oC)':[], 'Unidades Calor (oD)':[]}

# Se itera cada uno de los csv's para leerlos y almacenarlos en el diccionario
for file in archivos:
    # Se construye el path hacia el archivo y se crea el dataframe
    file_path = f"{data_folder}/{file}"
    current_df = readCsvAsDataFrame(file_path)
    # Se añade el archivo en su llave correspondiente del diccionario.
    # El elemento [1] de la lista de columnas current_df.columns corresponde al nombre de la variable en cuestión
    dfs_dict[current_df.columns[1]].append(current_df)
    
print("Se acaban de leer los csvs como dfs")
shutil.rmtree(data_folder) # Se elimina el folder de datos extraídos

# Una vez que se tienen todos los dataframes separados por variable, se concatenan para formar un solo dataframe por variable
for key in dfs_dict.keys():
    # Se obtiene el primer dataframe de la variable actual para poder correr la rutina de concatenación
    variable_df = dfs_dict[key].pop()
    # Se concatenan todos los demás dataframes en uno mismo 
    while dfs_dict[key]:
        variable_df = pd.concat([variable_df, dfs_dict[key].pop()])
    # Una vez que todos los dataframes estén concatenados se guarda el dataframe entero 
    # en el diccionario. Es decir, se reemplaza la lista de dataframes de la variable en cuestión 
    # por un solo dataframe
    dfs_dict[key] = variable_df

print("Se acaban de concatenar los dfs para formar un solo df por cada variable")

# En este punto ya tenemos un dataframe por cada variable. Ahora se va a realizar un outer join entre los 6 dataframes que representan
# a cada una de las variables
keys = [key for key in dfs_dict.keys()] # Se obtienen las llaves del diccionario, es decir, el nombre de cada variable
full_df = dfs_dict[keys.pop()] # Se toma algún dataframe para poder realizar los joins
# Mientras existan variables sin visitar, hacemos join del dataframe correspondiente a la variable con
# el full_df
while keys:
    full_df = pd.merge(full_df, dfs_dict[keys.pop()], on=['Fecha', 'IdEstacion', 'NombreEstacion', 'CoordGeoEstacion'], how='outer')

print("Se unieron todos los dataframes con outer joins")
# Se reordenan las columnas
ordered_columns = ['Fecha', 'IdEstacion', 'NombreEstacion', 'CoordGeoEstacion', 'Temp Mínima (oC)', 
                   'Temp Máxima (oC)', 'Temp Promedio (oC)', 'Unidades Calor (oD)', 'Evaporación (mm)', 'Precipitación (mm)']
full_df = full_df[ordered_columns]

print(full_df.head(10))

# Se guarda el archivo de resultados
full_df.to_csv(f'{data_path}/datos_sonora.csv', index=False)
# Se comprimen los resultados
with ZipFile(f'{data_path}/datos_sonora.zip', 'w') as zip:
    zip.write(f'{data_path}/datos_sonora.csv', compress_type=zipfile.ZIP_DEFLATED)
# Se elimina el csv
os.remove(f'{data_path}/datos_sonora.csv')
