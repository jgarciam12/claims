# -*- coding: utf-8 -*-
"""
Created on Wed Mar  1 12:18:13 2023

@author: Edgaviriac
"""


import dask.dataframe as dd
from datetime import datetime
import time
import pyttsx3
import pandas as pd

inicio = datetime.strptime(time.strftime("%Y%m%d %I.%M.%S %p"),"%Y%m%d %I.%M.%S %p")
print(str(inicio))

# Directorio de origen de los datos
origen = r"\\DC1PVFNAS1\Autos\BusinessIntelligence\21-CDM\DATOS"

# Leer los archivos parquet en DataFrames usando Dask y seleccionar columnas específicas
print('Leyendo datos generales')
datos_gene= dd.read_parquet(origen +r"\3-PQT\Datos_Generales\*.parquet",columns=["Mes Contable"])
print('Leyendo Reservas')
rsvas = dd.read_parquet(origen +r"\3-PQT\Rsvas\*.parquet",columns=["fuente","Mes Contable","sub_key","rsva","error"]) #
print('Leyendo key verify')
verify = dd.read_parquet(origen +r"\3-PQT\key_sub_verify.parquet")


# Bloque de cálculos y manipulaciones de datos
max_contab = datos_gene["Mes Contable"].max().compute()
print('Fecha máxima contable:',max_contab.date())
ant_contab = datos_gene["Mes Contable"].unique().nlargest(2).compute().iloc[-1]
print('Fecha contable cierre anterior:',ant_contab.date())
ly_contab = datos_gene["Mes Contable"].unique().nlargest(13).compute().iloc[-1]
print('Fecha contable hace un año:',ly_contab.date())




######################################################################################################################################
##########                                                     RESERVAS                                                     ##########
######################################################################################################################################



# Filtrar las reservas por mes contable
rsvas_act = rsvas[rsvas["Mes Contable"] == max_contab]
rsvas_ant = rsvas[rsvas["Mes Contable"] == ant_contab]
rsvas_ly = rsvas[rsvas["Mes Contable"] == ly_contab]

# tipos de reserva
fuente_rsv = rsvas["fuente"].unique().compute().tolist()

# Filtrar y manipular reservas anteriores
rsvas_ant = rsvas_ant[rsvas_ant["error"] =="OK"]
rsvas_ant["fuente"] = rsvas_ant["fuente"].astype("category")
rsvas_ant["fuente"] = rsvas_ant["fuente"].cat.as_known()
print('creando tabla pivot')

# Crear tabla pivote de reservas anteriores
rsva_ant = dd.pivot_table(rsvas_ant,index="sub_key", columns="fuente",values="rsva",aggfunc="sum").fillna(0)
rsva_ant["Valores.Total Rsva_Anterior"] = rsva_ant[fuente_rsv].sum(axis=1)
rsva_ant = rsva_ant.rename(columns={"Rsva-Gtos":"Valores.Rsva-Gtos_Anterior",
                          "Rsva-Hono":"Valores.Rsva-Hono_Anterior",
                          "Rsva-Indem":"Valores.Rsva-Indem_Anterior",
                          "Rsva-Recob":"Valores.Rsva-Recob_Anterior",
                          "Rsva-Salv":"Valores.Rsva-Salv_Anterior"                          
                          })

# Filtrar y manipular reservas actuales
rsvas_act = rsvas_act[rsvas_act["error"] =="OK"]
rsvas_act["fuente"] = rsvas_act["fuente"].astype("category")
rsvas_act["fuente"] = rsvas_act["fuente"].cat.as_known()

# Crear tabla pivote de reservas actuales
rsva_act = dd.pivot_table(rsvas_act,index="sub_key", columns="fuente",values="rsva",aggfunc="sum").fillna(0)
rsva_act["Valores.Total Rsvas_Actual"] = rsva_act[fuente_rsv].sum(axis=1)
rsva_act = rsva_act.rename(columns={"Rsva-Gtos":"Valores.Rsva-Gtos_Actual",
                          "Rsva-Hono":"Valores.Rsva-Hono_Actual",
                          "Rsva-Indem":"Valores.Rsva-Indem_Actual",
                          "Rsva-Recob":"Valores.Rsva-Recob_Actual",
                          "Rsva-Salv":"Valores.Rsva-Salv_Actual"                          
                          })


# Filtrar y manipular reservas de años anteriores
rsvas_ly= rsvas_ly[rsvas_ly["error"] =="OK"]
rsvas_ly["fuente"] = rsvas_ly["fuente"].astype("category")
rsvas_ly["fuente"] = rsvas_ly["fuente"].cat.as_known()

# Crear tabla pivote de reservas de años anteriores
rsva_ly = dd.pivot_table(rsvas_ly,index="sub_key", columns="fuente",values="rsva",aggfunc="sum").fillna(0)
rsva_ly["Valores.Total Rsvas_ly"] = rsva_ly[fuente_rsv].sum(axis=1)
rsva_ly = rsva_ly.rename(columns={"Rsva-Gtos":"Valores.Rsva-Gtos_ly",
                          "Rsva-Hono":"Valores.Rsva-Hono_ly",
                          "Rsva-Indem":"Valores.Rsva-Indem_ly",
                          "Rsva-Recob":"Valores.Rsva-Recob_ly",
                          "Rsva-Salv":"Valores.Rsva-Salv_ly"                          
                          })


# Combinar las tablas de reservas
tablarsvas = rsva_act.merge(rsva_ant,how="left",on="sub_key").merge(rsva_ly,how="left", on="sub_key")


#print("El valor total de Reservas es:"+str(tablarsvas['Valores.Rsva-Gtos_Actual'].sum()+tablarsvas['Valores.Rsva-Hono_Actual'].sum()+tablarsvas['Valores.Rsva-Indem_Actual'].sum()+tablarsvas['Valores.Rsva-Recob_Actual'].sum()+tablarsvas['Valores.Rsva-Salv_Actual'].sum()))


# Guardar la tabla de reservas en un archivo parquet
print('Guardando archivo')
tablarsvas.to_parquet(origen +r"/3-PQT\Tabla_Reservas\tabla_rsvas.parquet",compression="snappy")

tablarsvasInfo=pd.read_parquet(origen +r"/3-PQT\Tabla_Reservas\tabla_rsvas.parquet")
a = tablarsvasInfo['Valores.Rsva-Gtos_Actual'].sum() + tablarsvasInfo['Valores.Rsva-Hono_Actual'].sum() + tablarsvasInfo['Valores.Rsva-Indem_Actual'].sum() + tablarsvasInfo['Valores.Rsva-Recob_Actual'].sum() + tablarsvasInfo['Valores.Rsva-Salv_Actual'].sum()
print("La suma de la Reserva es: $", '{:,.0f}'.format(a))



fin = datetime.strptime(time.strftime("%Y%m%d %I.%M.%S %p"),"%Y%m%d %I.%M.%S %p")
tiempo_proceso = fin -inicio
print("fin proceso: " + time.strftime("%c"))
print("Tiempo total de procesamiento:\n" + str(tiempo_proceso))
engine = pyttsx3.init()
engine.say('El proceso a finalizado ')
engine.runAndWait()