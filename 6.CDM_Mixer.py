    # -*- coding: utf-8 -*-
"""
Created on Wed Mar  1 15:58:01 2023

@author: Edgaviriac
"""


import dask.dataframe as dd
from datetime import datetime
import time
import pyttsx3
import pandas as pd
import os



inicio = datetime.strptime(time.strftime("%Y%m%d %I.%M.%S %p"),"%Y%m%d %I.%M.%S %p")

# Ruta de origen de los datos
origen = r"\\DC1PVFNAS1\Autos\BusinessIntelligence\21-CDM\DATOS"

# Cargar datos generales desde archivos Parquet
print('Cargando Datos Generales')
datos_gene= dd.read_parquet(origen+r"\3-PQT\Datos_Generales\*.parquet")
print('Datos generales cargados \n')

# Cargar datos RCML desde archivos Parquet
print('Cargando datos RCL')
rcml = dd.read_parquet(origen+r"\3-PQT\RCML\*.parquet")
print('Datos RCML cargados \n')

# Cargar datos desde archivos Parquet
print('Cargando datos juridicos')
juridico= dd.read_parquet( origen+r"/3-PQT/Juridicos/*.parquet")
print('Datos juridicos cargados\n')

print('Cargando datos verify')
verify = dd.read_parquet(origen+r"\3-PQT\key_sub_verify.parquet")
print('Datos verify cargados\n')

print('Cargando datos fechas_contables')
fechas_contables = dd.read_parquet(origen +r"/3-PQT\Fecha_Contables\fechas_contables.parquet")
print('Datos fechas_contables cargados\n')

print('Cargando datos Pagos')
tablapagos = dd.read_parquet(origen +r"/3-PQT\Tabla_Pagos\tabla_pagos.parquet")
print('Datos Pagos cargados\n')

print('Cargando datos Recuperaciones')
tablarecup = dd.read_parquet(origen +r"/3-PQT\Tabla_Recuperaciones\tabla_recup.parquet")
print('Datos Recuperaciones cargados\n')

print('Cargando datos Reservas')
tablarsvas = dd.read_parquet(origen+r"\3-PQT\Tabla_Reservas\tabla_rsvas.parquet")
print('Datos Reservas cargados\n')
#%%
# Restablecer el índice de los datos RCML y eliminar duplicados por "sub_key"
rcml = rcml.reset_index()
rcml= rcml.drop_duplicates(subset=["sub_key"])
# Seleccionar columnas relevantes de los datos RCML
rcml=rcml[["sub_key","RCML"]]
# Eliminar duplicados en los datos jurídicos manteniendo la última entrada
juridico=juridico.drop_duplicates(subset=["sub_key"],keep="last")
# Agregar una columna "Caso_Juridico" con valor "JURIDICO" a los datos jurídicos
juridico["Caso_Juridico"] ="JURIDICO"

# Eliminar duplicados en los datos generales manteniendo la última entrada
datos_gene = datos_gene.drop_duplicates(subset=["sub_key"],keep="last")
#%%


# Fusionar los datos de diferentes fuentes en 'cdm'
print('Mezclando tablas')
cdm=datos_gene.merge(tablapagos, how="left", on ="sub_key"
                    ).merge(tablarecup,  how="left", on ="sub_key"
                            ).merge(tablarsvas, how="left", on ="sub_key"
                                    ).merge(rcml, how="left", on ="sub_key"
                                            ).merge(fechas_contables, how="left", on ="sub_key"
                                                    ).merge(juridico, how="left", on ="sub_key"
                                                            )#.merge(amp_autos, how="left", on="cod_ind_cob")
print('Tablas mezcladas\n')
#%%
  
# Eliminar duplicados en 'cdm' manteniendo la última entrada                                                            
cdm = cdm.drop_duplicates(subset="sub_key", keep="last")
print('Cantidad de registros: ', len(cdm))
print('\nCantidad de fechas contables no nulas: ',cdm['Mes Contable_y'].isnull().value_counts().compute())

#cdm = cdm.loc[(cdm["cod_ramo"] == "10") & (cdm["cod_ind_cob"].isin(['14','8','6','45','48','18','4','20','19','5','3','2','11','1']))]

# Guardar los datos finales en un archivo Parquet
#cant= len(var)
print('\nGuardando archivo total siniestros')                                                   
cdm.to_parquet(origen + '/archivo_total_stros.parquet',compression="snappy") 
print('Archivo total siniestros guardado')

#%%

cdm = pd.read_parquet(origen + '/archivo_total_stros.parquet')                                                   
print('\n')
print('Resumen: \n')
a = cdm['Valores.Pag-Gtos_Actual'].sum() + cdm['Valores.Pag-Hono_Actual'].sum() + cdm['Valores.Pag-Indem_Actual'].sum() + cdm['Valores.Pag-Recob_Actual'].sum() + cdm['Valores.Pag-Salv_Actual'].sum()
print("La suma total de Pagos es: $", '{:,.0f}'.format(a))
a = cdm['Valores.Rsva-Gtos_Actual'].sum()+cdm['Valores.Rsva-Hono_Actual'].sum()+cdm['Valores.Rsva-Indem_Actual'].sum()+cdm['Valores.Rsva-Recob_Actual'].sum()+cdm['Valores.Rsva-Salv_Actual'].sum()
print("La suma total de Reservas es: $", '{:,.0f}'.format(a))
a = cdm['Valores.Recup-Recob_Actual'].sum()+cdm['Valores.Recup-Salv_Actual'].sum()
print("La suma total de Recuperaciones es: $", '{:,.0f}'.format(a))
                                            
#%%

fin = datetime.strptime(time.strftime("%Y%m%d %I.%M.%S %p"),"%Y%m%d %I.%M.%S %p")
tiempo_proceso = fin -inicio
print("fin proceso: " + time.strftime("%c"))
print("Tiempo total de procesamiento:\n" + str(tiempo_proceso))
engine = pyttsx3.init()
engine.say('El proceso a finalizado ')
engine.runAndWait()
