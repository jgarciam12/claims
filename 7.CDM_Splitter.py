# -*- coding: utf-8 -*-
"""
Created on Sun Mar  5 15:07:37 2023

@author: Edgaviriac
"""

import dask.dataframe as dd
import time
from datetime import datetime 
import pyttsx3

# Definir la ruta de la carpeta de entrada
path_entrada = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\21-CDM\DATOS'

inicio = datetime.strptime(time.strftime("%Y%m%d %I.%M.%S %p"),"%Y%m%d %I.%M.%S %p")

# Leer el archivo parquet usando Dask DataFrame
print('Leyendo el archivo: archivo_total_stros')
mix = dd.read_parquet(path_entrada + "/archivo_total_stros.parquet")
print('Archivo archivo_total_stros leído\n')

#%%
cdmInfo=mix.compute()
a = cdmInfo['Valores.Pag-Gtos_Actual'].sum() + cdmInfo['Valores.Pag-Hono_Actual'].sum() + cdmInfo['Valores.Pag-Indem_Actual'].sum() + cdmInfo['Valores.Pag-Recob_Actual'].sum() + cdmInfo['Valores.Pag-Salv_Actual'].sum()
print("La suma total de Pagos es: $", '{:,.0f}'.format(a))
a = cdmInfo['Valores.Rsva-Gtos_Actual'].sum()+cdmInfo['Valores.Rsva-Hono_Actual'].sum()+cdmInfo['Valores.Rsva-Indem_Actual'].sum()+cdmInfo['Valores.Rsva-Recob_Actual'].sum()+cdmInfo['Valores.Rsva-Salv_Actual'].sum()
print("La suma total de Reservas es: $", '{:,.0f}'.format(a))
a = cdmInfo['Valores.Recup-Recob_Actual'].sum()+cdmInfo['Valores.Recup-Salv_Actual'].sum()
print("La suma total de Recuperaciones es: $", '{:,.0f}'.format(a))
del(cdmInfo)
# Convertir la columna de fecha en formato datetime
mix["fec_ocurrido"] = dd.to_datetime(mix["fec_ocurrido"],dayfirst = True) 

# Crear una nueva columna con el año de ocurrencia
mix["año_ocurrencia"] = mix["fec_ocurrido"].dt.strftime("%Y").fillna(2015).astype(int)

# Inicializar el valor del año de archivo
mix["año_archivo"] = int(2015)
# Aplicar una función para determinar el año de archivo adecuado
mix["año_archivo"] = mix.apply(
                        lambda row: 
                                row["año_ocurrencia"] if  row["año_ocurrencia"] > 2015
                                else row["año_archivo"]
                                , axis=1, meta ="object").fillna(2015).astype(int)

 # Año de inicio
start = 2015

# Procesar los datos por año y guardar en archivos parquet
print('\n')
while start <= 2024:
    print('Guardando archivo: Mixxed_', start)
    data = mix[mix["año_archivo"]==start]
    nombre = path_entrada + r"\Por año siniestro\Mixxed_" + str(start) + ".parquet"
    data.to_parquet(nombre, compression="snappy")
    print('Archivo Mixxed_', start, ' guardado\n')
    start += 1




fin = datetime.strptime(time.strftime("%Y%m%d %I.%M.%S %p"),"%Y%m%d %I.%M.%S %p")
tiempo_proceso = fin -inicio
print("fin proceso: " + time.strftime("%c"))
print("Tiempo total de procesamiento:\n" + str(tiempo_proceso))
engine = pyttsx3.init()
engine.say('El proceso a finalizado ')
engine.runAndWait()