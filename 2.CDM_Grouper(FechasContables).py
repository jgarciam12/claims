# -*- coding: utf-8 -*-
"""
Created on Tue Feb 28 22:30:01 2023

@author: Edgaviriac
"""

import dask.dataframe as dd
from datetime import datetime
import time
import pyttsx3
import os

inicio = datetime.strptime(time.strftime("%Y%m%d %I.%M.%S %p"),"%Y%m%d %I.%M.%S %p")

# Directorio de origen de los datos
origen = r"\\DC1PVFNAS1\Autos\BusinessIntelligence\21-CDM\DATOS"

# Leer los archivos parquet en DataFrames usando Dask
"""
print("leyendo archivos datos generales")

dic = []

for i in os.listdir(origen +r'\3-PQT\Datos_Generales'):
    print(i)
    df = dd.read_parquet(origen +r'\3-PQT\Datos_Generales' + '/' + i)
    dic.append(df)
datos_gene = dd.concat(dic)

#datos_gene = dd.read_parquet(origen +r"\3-PQT\Datos_Generales\*.parquet")
print("archivos datos generales leidos\n")
"""
print("leyendo archivos de pagos")
pagos = dd.read_parquet(origen +r"\3-PQT\Pag\*.parquet", columns = ["sub_key","Mes Contable"])
print("archivos de pagos leidos\n")

print("leyendo archivos de recuperaciones")
recup = dd.read_parquet(origen +r"\3-PQT\Recup\*.parquet", columns = ["sub_key","Mes Contable"])
print("archivos de recuperaciones leidos\n")

print("leyendo archivos de reservas")
rsvas = dd.read_parquet(origen +r"\3-PQT\Rsvas\*.parquet", columns = ["sub_key","Mes Contable"])
print("archivos de reservas leidos\n")

#print("leyendo archivos RCM")
#rcml = dd.read_parquet(origen +"r\3-PQT\RCML\*.parquet")
#print("archivos de RCML leidos\n")

#print("leyendo archivos Verify")
#verify = dd.read_parquet(origen +r"\3-PQT\key_sub_verify.parquet")
#print("archivos de verify leidos\n")
#%%


######################################################################################################################################
##########                                                  FECHAS CONTABLES                                                ##########
######################################################################################################################################

# Filtrar y combinar los DataFrames relacionados con las fechas contables
#pagcont= pagos[["sub_key","Mes Contable"]]
#reccont= recup[["sub_key","Mes Contable"]]
#rsvcont= rsvas[["sub_key","Mes Contable"]]
mcypm = dd.concat([rsvas,recup], axis=0)
mcypm = dd.concat([mcypm,pagos],axis=0)

# Calcular la última fecha contable y la primera fecha de movimiento por sub_key
mescontable = mcypm.groupby("sub_key")["Mes Contable"].max().reset_index()
primermov = mcypm.groupby("sub_key")["Mes Contable"].min().reset_index()
primermov = primermov.rename(columns={"Mes Contable":"Primer Movimiento"})

# Calcular las fechas máximas y anteriores de los datos generales
#max_contab = datos_gene["Mes Contable"].max().compute()
#ant_contab = datos_gene["Mes Contable"].unique().nlargest(2).compute().iloc[-1]

# Combinar las fechas contables y guardarlas en un archivo parquet
fechas_contables =  mescontable.merge(primermov, how="left", on ="sub_key")
fechas_contables[fechas_contables.columns].to_parquet(origen + r"/3-PQT\Fecha_Contables\fechas_contables.parquet",compression="gzip",engine="pyarrow")



fin = datetime.strptime(time.strftime("%Y%m%d %I.%M.%S %p"),"%Y%m%d %I.%M.%S %p")
tiempo_proceso = fin -inicio
print("fin proceso: " + time.strftime("%c"))
print("Tiempo total de procesamiento:\n" + str(tiempo_proceso))
engine = pyttsx3.init()
engine.say('El proceso a finalizado ')
engine.runAndWait()

