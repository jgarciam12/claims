# -*- coding: utf-8 -*-
"""
Created on Mon Mar  6 14:12:16 2023

@author: Edgaviriac
"""

import dask.dataframe as dd
from datetime import datetime
import time
import pyttsx3
import pandas as pd
import gc

inicio = datetime.strptime(time.strftime("%Y%m%d %I.%M.%S %p"),"%Y%m%d %I.%M.%S %p")

# Definir rutas de directorios

path = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\21-CDM\DATOS\Formatos'
path2 = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\21-CDM\DATOS\4-Salidas\1-Datos_por_ocurrencia\1-Autos_Sin_Asistencias'
path3 = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\21-CDM\DATOS\4-Salidas\1-Datos_por_ocurrencia\2-Asistencias'
path4 = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\21-CDM\DATOS\4-Salidas\1-Datos_por_ocurrencia\3-Generales_y_Vida'
path5 = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\21-CDM\DATOS\4-Salidas\1-Datos_por_ocurrencia\4-Soat'
path6 = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\21-CDM\DATOS\4-Salidas\2-Consolidados'


# Leer campos desde archivo CSV usando Dask
campos= dd.read_csv(path + r"\Campos CDM.csv",sep=";",encoding="latin1")
camposcons_stros = campos[campos["consultagyv"]=="incluir"]
camposcons_stros = camposcons_stros["Campo"]
camposcons_autos = campos[campos["consultaautos"]=="incluir"]
camposcons_autos = camposcons_autos["Campo"]
campos = campos["Campo"]
#%%
# Leer archivos CSV utilizando Dask y procesar los datos
autos = dd.read_csv(path2 + r"\*.csv", sep=";",encoding="latin1",dtype="object",usecols=campos)
autos = autos.reset_index()
autos = autos.compute()
autos = autos[campos]
#%%
asistencias = dd.read_csv(path3 + r"\*.csv", sep=";",encoding="latin1",dtype="object",usecols=campos)
asistencias = asistencias.reset_index()
asistencias = asistencias.compute()
asistencias = asistencias[campos]
#%%
gyv = dd.read_csv(path4 + r"\*.csv", sep=";",encoding="latin1",dtype="object",usecols=campos)

gyv = gyv.reset_index()
gyv = gyv.compute()
gyv = gyv[campos]
#%%
soat = dd.read_csv(path5 + r"\*.csv", sep=";",encoding="latin1",dtype="object",usecols=campos)
soat = soat.reset_index()
soat = soat.compute()
soat = soat[campos]
#%%

# Guardar los resultados en archivos CSV consolidados
print('Guardando archivo 5-Subsiniestros-Claims_Data_Manager-Solo_Autos')
autos.to_csv(path6 + r"\5-Subsiniestros-Claims_Data_Manager-Solo_Autos.csv",sep=";",decimal=",",encoding="latin1", index=None)
print('Archivo 5-Subsiniestros-Claims_Data_Manager-Solo_Autos guardado\n')

print('Asistencias: \n')
print('Pagos: ${:,.0f}'.format(autos['Valores.Total Pagos_Actual'].astype(str).str.replace(',','.').astype(float).sum()))
print('Recuperaciones: ${:,.0f}'.format(autos['Valores.Total Recup_Actual'].astype(str).str.replace(',','.').astype(float).sum()))
print('Reserva: ${:,.0f}'.format(autos['Valores.Total Rsva_Actual'].astype(str).str.replace(',','.').astype(float).sum()),'\n')

print('Guardando archivo 2-Subsiniestros-Claims_Data_Manager-Asistencias')
asistencias.to_csv(path6 + r"\2-Subsiniestros-Claims_Data_Manager-Asistencias.csv",sep=";",decimal=",",encoding="latin1", index=None)
print('Archivo 2-Subsiniestros-Claims_Data_Manager-Asistencias guardado\n')

print('Guardando archivo 4-Subsiniestros-Claims_Data_Manager-Solo_Gles_y_Vida')
gyv.to_csv(path6 + r"\4-Subsiniestros-Claims_Data_Manager-Solo_Gles_y_Vida.csv",sep=";",decimal=",",encoding="latin1", index=None)
print('Archivo 4-Subsiniestros-Claims_Data_Manager-Solo_Gles_y_Vida guardado\n')
#%%
print('Guardando archivo 3-Subsiniestros-Claims_Data_Manager-Soat')
soat.to_csv(path6 + r"\3-Subsiniestros-Claims_Data_Manager-Soat.csv",sep=";",decimal=",",encoding="latin1", index=None)
print('Archivo 3-Subsiniestros-Claims_Data_Manager-Soat guardado\n')

# Concatenar y procesar los datos
agv = pd.concat([autos,gyv],axis=0)
agva = pd.concat([agv,asistencias], axis=0) 

agva["Mes Contable_y"] = pd.to_datetime(agva["Mes Contable_y"],dayfirst=True)
agva["Año_Contable"] = agva["Mes Contable_y"].dt.strftime("%Y").fillna("0").astype(int)
agva = agva[agva["Año_Contable"] > 2018]
agva = agva[camposcons_stros]

# Liberar memoria
del soat
del gyv
gc.collect()

# Guardar el resultado de la consulta en archivo CSV
print('Guardando archivo 19-Subsiniestros-Claims_Data_Manager-Consulta_Gles_y_Vida')
agva.to_csv(path6 + r"\19-Subsiniestros-Claims_Data_Manager-Consulta_Gles_y_Vida.csv",sep=";",decimal=",",encoding="latin1", index=None)
print('Archivo 19-Subsiniestros-Claims_Data_Manager-Consulta_Gles_y_Vida guardado\n')

print('Generales: \n')
print('Pagos: ${:,.0f}'.format(agv.loc[agv['Base'] == 'Generales','Valores.Total Pagos_Actual'].astype(str).str.replace(',','.').astype(float).sum()))
print('Recuperaciones: ${:,.0f}'.format(agv.loc[agv['Base'] == 'Generales','Valores.Total Recup_Actual'].astype(str).str.replace(',','.').astype(float).sum()))
print('Reserva: ${:,.0f}'.format(agv.loc[agv['Base'] == 'Generales','Valores.Total Rsva_Actual'].astype(str).str.replace(',','.').astype(float).sum()))

print('\n')
print('Total: \n')
print('Pagos: ${:,.0f}'.format(autos['Valores.Total Pagos_Actual'].astype(str).str.replace(',','.').astype(float).sum() + agv.loc[agv['Base'] == 'Generales','Valores.Total Pagos_Actual'].astype(str).str.replace(',','.').astype(float).sum()))
print('Recuperaciones: ${:,.0f}'.format(autos['Valores.Total Recup_Actual'].astype(str).str.replace(',','.').astype(float).sum() + agv.loc[agv['Base'] == 'Generales','Valores.Total Recup_Actual'].astype(str).str.replace(',','.').astype(float).sum()))
print('Reserva: ${:,.0f}'.format(autos['Valores.Total Rsva_Actual'].astype(str).str.replace(',','.').astype(float).sum() + agv.loc[agv['Base'] == 'Generales','Valores.Total Rsva_Actual'].astype(str).str.replace(',','.').astype(float).sum()))

print('Guardando archivo 1-Subsiniestros-Claims_Data_Manager-Autos_Gles_y_Vida')
agv.to_csv(path6 + r"\1-Subsiniestros-Claims_Data_Manager-Autos_Gles_y_Vida.csv",sep=";",decimal=",",encoding="latin1", index=None)
print('Archivo 1-Subsiniestros-Claims_Data_Manager-Autos_Gles_y_Vida guardado\n')

# Liberar memoria
del agv
del agva
gc.collect()

# Filtrar y guardar datos específicos
asist_autos = asistencias[asistencias["cod_ramo"].isin(["10","12"])]
autosyasis = pd.concat([autos,asist_autos],axis=0)
autoscons = autosyasis
autoscons["Mes Contable_y"] = pd.to_datetime(autoscons["Mes Contable_y"],dayfirst=True)
autoscons["Año_Contable"] = autoscons["Mes Contable_y"].dt.strftime("%Y").fillna("0").astype(int)
autoscons = autoscons[autoscons["Año_Contable"] > 2018]
#autoscons = autoscons.rename(columns={"sub_key":"MaestroCobertura"})
autoscons = autoscons[camposcons_autos]
# Guardar resultados en archivo CSV
print('Guardando archivo 20-Subsiniestros-Claims_Data_Manager-Consulta_Autos')
autoscons.to_csv(path6 + r"\20-Subsiniestros-Claims_Data_Manager-Consulta_Autos.csv",sep=";",decimal=",",encoding="latin1", index=None)
print('Archivo 20-Subsiniestros-Claims_Data_Manager-Consulta_Autos guardado\n')

# Liberar memoria
del asistencias
del autos
gc.collect()

# Guardar datos resultantes en archivo CSV
print('Guardando archivo 6-Subsiniestros-Claims_Data_Manager-Autos_con_Asistencias')
autosyasis.to_csv(path6 + r"\6-Subsiniestros-Claims_Data_Manager-Autos_con_Asistencias.csv",sep=";",decimal=",",encoding="latin1", index=None)
print('Archivo 6-Subsiniestros-Claims_Data_Manager-Autos_con_Asistencias\n')

fin = datetime.strptime(time.strftime("%Y%m%d %I.%M.%S %p"),"%Y%m%d %I.%M.%S %p")
tiempo_proceso = fin -inicio
print("fin proceso: " + time.strftime("%c"))
print("Tiempo total de procesamiento:\n" + str(tiempo_proceso))
engine = pyttsx3.init()
engine.say('El proceso a finalizado ')
engine.runAndWait() 