# -*- coding: utf-8 -*-
"""
Created on Wed Mar 15 14:10:18 2023

@author: Edgaviriac
"""


####REPORTEADOR MENSUAL####



import dask.dataframe as dd
import gc
import time
import pyttsx3
from datetime import datetime

path = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\21-CDM\DATOS'

def dectx_toint(row):
    return row.fillna("0,0").str.replace(",",".").astype(float).astype(int)

inicio = datetime.strptime(time.strftime("%Y%m%d %I.%M.%S %p"),"%Y%m%d %I.%M.%S %p")

columns_names_generic = dd.read_csv(path + r"\Formatos\Columnas_intermediarios.csv",encoding = "latin1",sep=";",lineterminator=("\r\n"),quotechar='"',na_values=["","NA"],dayfirst=(True),low_memory=(False),decimal=",",dtype = "object")    
columns_names_merck = dd.read_csv(path + r"\Formatos\Columnas_merck.csv",encoding = "latin1",sep=";",lineterminator=("\r\n"),quotechar='"',na_values=["","NA"],dayfirst=(True),low_memory=(False),decimal=",",dtype = "object")    


gyv=dd.read_csv(path + r"\4-Salidas\2-Consolidados\1-Subsiniestros-Claims_Data_Manager-Autos_Gles_y_Vida.csv",encoding = "latin1",sep=";",lineterminator=("\r\n"),quotechar='"',na_values=["","NA"],dayfirst=(True),low_memory=(False),decimal=",",dtype = "object")    
asis=dd.read_csv(path + r"\4-Salidas\2-Consolidados\2-Subsiniestros-Claims_Data_Manager-Asistencias.csv",encoding = "latin1",sep=";",lineterminator=("\r\n"),quotechar='"',na_values=["","NA"],dayfirst=(True),low_memory=(False),decimal=",",dtype = "object")    
soat = dd.read_csv(path + r"\4-Salidas\2-Consolidados\3-Subsiniestros-Claims_Data_Manager-Soat.csv",encoding = "latin1",sep=";",lineterminator=("\r\n"),quotechar='"',na_values=["","NA"],dayfirst=(True),low_memory=(False),decimal=",",dtype = "object")
complete= gyv.append(asis)
complete= complete.append(soat)


complete["cod_agente"] = dectx_toint(complete["cod_agente"]).astype(str)

complete = complete.compute()

del asis
del gyv
gc.collect()


#######################################################################################################################################################################################
#####################################################################               MASIVOS             ###############################################################################
#######################################################################################################################################################################################

#complete = complete[columns_names_generic.columns]
complete["Segmento"] = complete["Segmento"].str.replace("Complesjos","Complejos")
complete["ruta"] = path + r"\4-Salidas\3-Masivos\2-Seguimiento_a_Reservas\Reservas_" + complete["Segmento"] + ".csv"
complete=complete[dectx_toint(complete["Valores.Total Rsva_Actual"])>0]


ruta = complete[["ruta"]]
ruta = ruta.drop_duplicates().dropna()
ruta = ruta["ruta"].tolist()

complete.to_csv(path + r"\4-Salidas\3-Masivos\2-Seguimiento_a_Reservas\Reservas_Total.csv",encoding = "latin1",sep=";",decimal=",",index=None)


complete=complete[(complete["Alerta_param"] == "Si")|(complete["Alerta_anual"]=="Si")]
###################    MASIVOS POR CODIGO AGENTE    

for guardar in ruta:
    
    indiv = complete[complete["ruta"]==guardar]
    #indiv.drop(columns=["ruta_int","ruta_nit","cuenta_intermediario","Codigo Agente","Nit","cuenta_documento"])
    #indiv = indiv[columns_names_generic.columns]
    print('Guardando archivo: ',guardar)
    indiv.to_csv(guardar,
            encoding = "latin1",sep=";",decimal=",",index=None)
    print('Archivo ',guardar, ' guardado')
    del indiv
    gc.collect()

 



fin = datetime.strptime(time.strftime("%Y%m%d %I.%M.%S %p"),"%Y%m%d %I.%M.%S %p")
tiempo_proceso = fin -inicio
print("fin proceso: " + time.strftime("%c"))
print("Tiempo total de procesamiento:\n" + str(tiempo_proceso))
engine = pyttsx3.init()
engine.say('El proceso a finalizado ')
engine.runAndWait() 