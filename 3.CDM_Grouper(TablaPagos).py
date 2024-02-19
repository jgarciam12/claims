# -*- coding: utf-8 -*-
"""
Created on Wed Mar  1 10:58:09 2023

@author: Edgaviriac
"""
import dask.dataframe as dd
from datetime import datetime
import time
import pyttsx3
import pandas as pd


inicio = datetime.strptime(time.strftime("%Y%m%d %I.%M.%S %p"),"%Y%m%d %I.%M.%S %p")

#leyendo Directorio de origen de los datos
origen = r"\\DC1PVFNAS1\Autos\BusinessIntelligence\21-CDM\DATOS"


#Leyendo los archivos parquet en DataFrames usando Dask y seleccionar columnas específicas
datos_gene= dd.read_parquet(origen + r"\3-PQT\Datos_Generales\*.parquet",columns=["Mes Contable"])
pagos = dd.read_parquet(origen + r"\3-PQT\Pag\*.parquet",columns=["fuente","nom_concepto","Mes Contable","sub_key","Detalle_SP","pag","fec_emi","fec_formalizado"])
verify = dd.read_parquet(origen + r"\3-PQT\key_sub_verify.parquet")


#%%
# Bloque de cálculos y manipulaciones de datos
max_contab = datos_gene["Mes Contable"].max().compute()
ant_contab = datos_gene["Mes Contable"].unique().nlargest(2).compute().iloc[-1]
ly_contab = datos_gene["Mes Contable"].unique().nlargest(13).compute().iloc[-1]
#%%


######################################################################################################################################
##########                                                     PAGOS                                                        ##########
######################################################################################################################################


#%%
fuente_pag = pagos["fuente"].unique().compute().tolist()
concepto_pag =  pagos["nom_concepto"].unique().compute().tolist()
#%%
# Bloque de cálculos y manipulaciones de datos relacionados con los pagos
pagos["fuente"] = pagos["fuente"].astype("category")
pagos["fuente"] = pagos["fuente"].cat.as_known()
pagos["nom_concepto"] = pagos["nom_concepto"].astype("category")
pagos["nom_concepto"] = pagos["nom_concepto"].cat.as_known()
#%%
# Filtrar las filas con valores nulos en la columna 'fec_emi'
pagos = pagos[pagos['fec_emi'].isnull() == False]
pagos = pagos.sort_values(by="fec_emi",ascending=True) 
pagos2 = pagos[pagos['fec_emi'].isnull() == True]
print(len(pagos2))
#%%

# Verificar si existen valores nulos en 'fec_emi'
nulos = pagos['fec_emi'].isna().any().compute()

if nulos:
    print('Si Hay')
#%%

# Crear una tabla pivote de pagos
pag = dd.pivot_table(pagos,index="sub_key", columns="fuente",values="pag",aggfunc="sum").fillna(0)
pag["Valores.Total Pagos_Final"] = pag[fuente_pag].sum(axis=1)
pag = pag.rename(columns={"Pag-Gtos":"Valores.Pag-Gtos_Final",
                          "Pag-Hono":"Valores.Pag-Hono_Final",
                          "Pag-Indem":"Valores.Pag-Indem_Final",
                          "Pag-Recob":"Valores.Pag-Recob_Final",
                          "Pag-Salv":"Valores.Pag-Salv_Final"                          
                          })



pagos_act = pagos[pagos["Mes Contable"] == max_contab]


pagos_act["fuente"] = pagos_act["fuente"].astype("category")
pagos_act["fuente"] = pagos_act["fuente"].cat.as_known()
pagos_act["nom_concepto"] = pagos_act["nom_concepto"].astype("category")
pagos_act["nom_concepto"] = pagos_act["nom_concepto"].cat.as_known()
pagos_act = pagos_act.sort_values(by="fec_emi",ascending=True) 
pag_act = dd.pivot_table(pagos_act,index="sub_key", columns="fuente",values="pag",aggfunc="sum").fillna(0)
pag_act["Valores.Total Pagos_Actual"] = pag_act[fuente_pag].sum(axis=1)
pag_act = pag_act.rename(columns={"Pag-Gtos":"Valores.Pag-Gtos_Actual",
                          "Pag-Hono":"Valores.Pag-Hono_Actual",
                          "Pag-Indem":"Valores.Pag-Indem_Actual",
                          "Pag-Recob":"Valores.Pag-Recob_Actual",
                          "Pag-Salv":"Valores.Pag-Salv_Actual"                          
                          })




pagos_ly = pagos[pagos["Mes Contable"] == ly_contab]


pagos_ly["fuente"] = pagos_ly["fuente"].astype("category")
pagos_ly["fuente"] = pagos_ly["fuente"].cat.as_known()
pagos_ly["nom_concepto"] = pagos_ly["nom_concepto"].astype("category")
pagos_ly["nom_concepto"] = pagos_ly["nom_concepto"].cat.as_known()
pagos_ly = pagos_ly.sort_values(by="fec_emi",ascending=True) 
pag_ly = dd.pivot_table(pagos_ly,index="sub_key", columns="fuente",values="pag",aggfunc="sum").fillna(0)
pag_ly["Valores.Total Pagos_ly"] = pag_ly[fuente_pag].sum(axis=1)
pag_ly = pag_ly.rename(columns={"Pag-Gtos":"Valores.Pag-Gtos_ly",
                          "Pag-Hono":"Valores.Pag-Hono_ly",
                          "Pag-Indem":"Valores.Pag-Indem_ly",
                          "Pag-Recob":"Valores.Pag-Recob_ly",
                          "Pag-Salv":"Valores.Pag-Salv_ly"                          
                          })

#%%
pagfirst = pagos.copy().compute() #######
pagfirst = pagfirst.pivot_table(index="sub_key", columns="nom_concepto",values="fec_emi",aggfunc='first') #######
#pagfirst = pagos.pivot_table(index="sub_key", columns="nom_concepto",values="fec_emi",aggfunc='first')


#%%
pagfirst = pagfirst.rename(columns={"Gastos Recobro":"First_Gastos_Recobro",
                          "Gastos Salvamento":"First_Gastos_Salvamento",
                          "Indemnizacion":"First_Indemnizacion",
                          "Honorarios Stros":"First_Honorarios_Stros",
                          "Gastos Stros":"First_Gastos_Stros"                          
                          })

paglast = pagos.copy().compute()
paglast = paglast.pivot_table(index="sub_key", columns="nom_concepto",values="fec_emi",aggfunc="last") #######
#%%
#paglast = pagos.pivot_table(index="sub_key", columns="nom_concepto",values="fec_emi",aggfunc="last")

paglast = paglast.rename(columns={"Gastos Recobro":"Last_Gastos_Recobro",
                          "Gastos Salvamento":"Last_Gastos_Salvamento",
                          "Indemnizacion":"Last_Indemnizacion",
                          "Honorarios Stros":"Last_Honorarios_Stros",
                          "Gastos Stros":"Last_Gastos_Stros"                          
                          })
#%%
detpag= pagos.groupby("sub_key")["Detalle_SP"].apply(lambda x:"|".join(x.astype(str))).reset_index()
detpag = detpag.rename(columns={"Detalle_SP":"Detalle_ Pagos"})
detindem = pagos[pagos["nom_concepto"]=="Indemnizacion"]
detindem = detindem.groupby("sub_key")["Detalle_SP"].apply(lambda x:"|".join(x.astype(str))).reset_index()
detindem = detindem.rename(columns={"Detalle_SP":"Detalle_indemnizacion"})
#%%
formfirst = pagos.groupby("sub_key")["fec_formalizado"].first().reset_index()
formfirst = formfirst.rename(columns={"fec_formalizado":"1ra_Fec_Formalizado"})
#%%
print('Mezclando tablas')
tablapagos = pag.merge(pag_act,how="left",on="sub_key"
                           ).merge(pagfirst,how="left",on="sub_key"
                                   ).merge(paglast,how="left",on="sub_key"
                                           ).merge(detpag,how="left",on="sub_key"
                                                   ).merge(formfirst,how="left",on="sub_key"
                                                           ).merge(detindem,how="left",on="sub_key"
                                                                   ).merge(pag_ly,how="left",on="sub_key"
                                                                           )
print('Tablas mezcladas\n')
#%%
#print("La suma de los Pagos es:"+str(tablapagos['Valores.Pag-Gtos_Actual'].sum()+tablapagos['Valores.Pag-Hono_Actual'].sum()+tablapagos['Valores.Pag-Indem_Actual'].sum()+tablapagos['Valores.Pag-Recob_Actual'].sum()+tablapagos['Valores.Pag-Salv_Actual'].sum()))

# Guardar la tabla de pagos en un archivo parquet
print('Guardando archivo de Pagos')
tablapagos.to_parquet(origen +r"/3-PQT\Tabla_Pagos\tabla_pagos.parquet",compression="snappy")
print('Archivo de Pagos guardado\n')

tablapagosInfo=pd.read_parquet(origen +r"/3-PQT\Tabla_Pagos\tabla_pagos.parquet")

a = tablapagosInfo['Valores.Pag-Gtos_Actual'].sum() + tablapagosInfo['Valores.Pag-Hono_Actual'].sum() + tablapagosInfo['Valores.Pag-Indem_Actual'].sum() + tablapagosInfo['Valores.Pag-Recob_Actual'].sum() + tablapagosInfo['Valores.Pag-Salv_Actual'].sum()
print('Resumen: \n')
print("La suma de los Pagos es: $", '{:,.0f}'.format(a))



fin = datetime.strptime(time.strftime("%Y%m%d %I.%M.%S %p"),"%Y%m%d %I.%M.%S %p")
tiempo_proceso = fin -inicio
print("fin proceso: " + time.strftime("%c"))
print("Tiempo total de procesamiento:\n" + str(tiempo_proceso))
engine = pyttsx3.init()
engine.say('El proceso a finalizado ')
engine.runAndWait()