# -*- coding: utf-8 -*-
"""
Created on Wed Mar  1 12:36:39 2023

@author: Edgaviriac
"""


import dask.dataframe as dd
from datetime import datetime
import time
import pyttsx3
import pandas as pd

inicio = datetime.strptime(time.strftime("%Y%m%d %I.%M.%S %p"),"%Y%m%d %I.%M.%S %p")

# Ruta de origen de los datos
origen = r"\\DC1PVFNAS1\Autos\BusinessIntelligence\21-CDM\DATOS"

# Leer los datos generales y las recuperaciones desde archivos parquet
datos_gene= dd.read_parquet(origen +r"\3-PQT\Datos_Generales\*.parquet",columns=["Mes Contable"])
recup = dd.read_parquet(origen +r"\3-PQT\Recup\*.parquet",columns=["fuente","nom_concepto","Mes Contable","sub_key","recup","fec_emi"])#
verify = dd.read_parquet(origen +r"\3-PQT\key_sub_verify.parquet")


# Calcular valores máximos y anteriores para Mes Contable
max_contab = datos_gene["Mes Contable"].max().compute()
ant_contab = datos_gene["Mes Contable"].unique().nlargest(2).compute().iloc[-1]
ly_contab = datos_gene["Mes Contable"].unique().nlargest(13).compute().iloc[-1]


######################################################################################################################################
##########                                                 RECUPERACIONES                                                   ##########
######################################################################################################################################

# Filtrar las recuperaciones para el Mes Contable máximo y el Mes Contable de hace 13 meses
recup_act = recup[recup["Mes Contable"] == max_contab]
recup_ly = recup[recup["Mes Contable"] == ly_contab]

# Obtener las fuentes y conceptos únicos de las recuperaciones
fuente_rec = recup["fuente"].unique().compute().tolist()
concepto_rec =  recup["nom_concepto"].unique().compute().tolist()

# Convertir las columnas "fuente" y "nom_concepto" en categorías y asignar valores conocidos
recup["fuente"] = recup["fuente"].astype("category")
recup["fuente"] = recup["fuente"].cat.as_known()
recup["nom_concepto"] = recup["nom_concepto"].astype("category")
recup["nom_concepto"] = recup["nom_concepto"].cat.as_known()
# Ordenar las recuperaciones por la fecha de emisión ("fec_emi") de manera ascendente
recup = recup.sort_values(by="fec_emi",ascending=True)

# Generar una tabla pivotante que suma las recuperaciones por "sub_key" y "fuente",
# y luego llenar los valores faltantes con cero
rec = dd.pivot_table(recup,index="sub_key", columns="fuente",values="recup",aggfunc="sum").fillna(0)
# Calcular la columna "Valores.Total Recup_Final" sumando los valores de las fuentes en cada fila
rec["Valores.Total Recup_Final"] = rec[fuente_rec].sum(axis=1)
# Renombrar las columnas para reflejar los tipos de recuperación específicos
rec = rec.rename(columns={"Recup-Recob":"Valores.Recup-Recob_Final",
                          "Recup-Salv":"Valores.Recup-Salv_Final"                      
                          })

# Repetir el proceso de preparación y cálculos similares para las recuperaciones actuales (recup_act) y pasadas (recup_ly)

recup_act["fuente"] = recup_act["fuente"].astype("category")
recup_act["fuente"] = recup_act["fuente"].cat.as_known()
recup_act["nom_concepto"] = recup_act["nom_concepto"].astype("category")
recup_act["nom_concepto"] = recup_act["nom_concepto"].cat.as_known()
recup_act = recup_act.sort_values(by="fec_emi",ascending=True)
rec_act = dd.pivot_table(recup_act,index="sub_key", columns="fuente",values="recup",aggfunc="sum").fillna(0)
rec_act["Valores.Total Recup_Actual"] = rec_act[fuente_rec].sum(axis=1)
rec_act = rec_act.rename(columns={"Recup-Recob":"Valores.Recup-Recob_Actual",
                          "Recup-Salv":"Valores.Recup-Salv_Actual"                      
                          })





recup_ly["fuente"] = recup_ly["fuente"].astype("category")
recup_ly["fuente"] = recup_ly["fuente"].cat.as_known()
recup_ly["nom_concepto"] = recup_ly["nom_concepto"].astype("category")
recup_ly["nom_concepto"] = recup_ly["nom_concepto"].cat.as_known()
recup_ly = recup_ly.sort_values(by="fec_emi",ascending=True)
rec_ly = dd.pivot_table(recup_ly,index="sub_key", columns="fuente",values="recup",aggfunc="sum").fillna(0)
rec_ly["Valores.Total Recup_ly"] = rec_ly[fuente_rec].sum(axis=1)
rec_ly = rec_ly.rename(columns={"Recup-Recob":"Valores.Recup-Recob_ly",
                          "Recup-Salv":"Valores.Recup-Salv_ly"                      
                          })


# Generar la tabla recfirst con la fecha de primer recobro y salvamento
recfirst = recup.copy().compute() ########
recfirst = recfirst.pivot_table(index="sub_key", columns="nom_concepto",values="fec_emi",aggfunc="first")
recfirst = recfirst.rename(columns={"Recobro":"First_Recobro",
                          "Salvamento":"First_Salvamento"                          
                          })

# Generar la tabla reclast con la fecha de último recobro y salvamento
reclast = recup.copy().compute() #######
reclast = reclast.pivot_table(index="sub_key", columns="nom_concepto",values="fec_emi",aggfunc="last")
reclast = reclast.rename(columns={"Recobro":"Last_Recobro",
                          "Salvamento":"Last_Salvamento"                          
                          })

#detrec= recup.groupby("sub_key")["Detalle_SP"].apply(lambda x:"|".join(x.astype(str))).reset_index()
#detrec = detrec.rename(columns={"Detalle_SP":"Detalle_ Pagos"})

# Generar y fusionar las tablas resultantes en 'tablarecup'
tablarecup = rec.merge(rec_act,how="left",on="sub_key"
                      ).merge(recfirst,how="left",on="sub_key"
                              ).merge(reclast,how="left",on="sub_key"
                                      ).merge(rec_ly,how="left", on="sub_key"
                                              )
                                              
#print("El valor total de Recuperaciones es:"+ str(tablarecup['Valores.Recup-Recob_Actual'].sum()+tablarecup['Valores.Recup-Salv_Actual'].sum()))
                                          
# Guardar la tabla de recuperaciones resultante en un archivo Parquet                                             
print('Guardando tabla de recuperaciones')        
tablarecup.to_parquet(origen +r"/3-PQT\Tabla_Recuperaciones\tabla_recup.parquet",compression="snappy")

tablarecupInfo=pd.read_parquet(origen +r"/3-PQT\Tabla_Recuperaciones\tabla_recup.parquet")
a = tablarecupInfo['Valores.Recup-Recob_Actual'].sum()+tablarecupInfo['Valores.Recup-Salv_Actual'].sum()
print("El valor total de Recuperaciones es: $",'{:,.0f}'.format(a))


fin = datetime.strptime(time.strftime("%Y%m%d %I.%M.%S %p"),"%Y%m%d %I.%M.%S %p")
tiempo_proceso = fin -inicio
print("fin proceso: " + time.strftime("%c"))
print("Tiempo total de procesamiento:\n" + str(tiempo_proceso))
engine = pyttsx3.init()
engine.say('El proceso a finalizado ')
engine.runAndWait()