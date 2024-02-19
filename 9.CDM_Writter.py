# -*- coding: utf-8 -*-
"""
Created on Sat Mar  4 21:17:02 2023

@author: Edgaviriac
"""



import pyttsx3
from datetime import datetime
import dask.dataframe as dd
import time
import re


inicio = datetime.strptime(time.strftime("%Y%m%d %I.%M.%S %p"),"%Y%m%d %I.%M.%S %p")

# Definir la ruta del directorio
path = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\21-CDM\DATOS'

# Leer el archivo CSV que contiene los campos
campos= dd.read_csv(path + r"\Formatos\Campos CDM.csv",sep=";",encoding="latin1")
campos = campos["Campo"]

# Definir los años de inicio y fin para el bucle
split = 2015
end= 2024


# Bucle para procesar cada año
while split <= end: 
    # Obtener la hora de inicio de la iteración actual
    print(split, '\n')
    iniciowh = datetime.strptime(time.strftime("%Y%m%d %I.%M.%S %p"),"%Y%m%d %I.%M.%S %p")
        

    
# Leer el archivo Parquet correspondiente al año actual
    
    print('leyendo datos: cdmtotal_' + str(split))
    cdmfin= dd.read_parquet(path + r"\3-PQT\CDM Total\cdmtotal_"+str(split)+".parquet")
    print('Datos cdmtotal_' + str(split), 'leídos\n')

    # Agregar una columna "es_asistencia" utilizando expresiones regulares para categorizar los registros
    cdmfin["es_asistencia"] =  cdmfin.apply(
                                    lambda row: "No" if re.match('.*JURIDICA.*', str(row["amparo"]) )
                                    else "No" if  re.match('.*EXEQUIAL.*', str(row["amparo"]) ) 
                                    else "Si" if re.match('.*ASISTENCIA.*', str(row["amparo"]))
                                    else "No"
                                , axis=1, meta ="object")
        # Cambiar el nombre de sub_key_x por sub_key
    cdmfin = cdmfin.rename(columns={"sub_key_x":"sub_key"})
    
    # Filtrar y procesar los registros para diferentes categorías y guardar los resultados en archivos CSV
    cdmautos = cdmfin.loc[(cdmfin["cod_ramo"].isin(["10","12"])) & (cdmfin["cod_ind_cob"].isin(['14','8','6','45','48','18','4','20','19','5','3','2','11','1']))]
    
    cdmautos = cdmautos.compute()
    print('Guardando archivo CDM_Autos_no_asis_' + str(split))
    cdmautos.to_csv(path + r"\4-Salidas\1-Datos_por_ocurrencia\1-Autos_Sin_Asistencias\CDM_Autos_no_asis_"+str(split)+".csv",sep=";",decimal=",",encoding="latin1", index=None)
    print('Archivo CDM_Autos_no_asis_' + str(split), 'guardado\n')

    cdmasistencias = cdmfin.loc[cdmfin["es_asistencia"] == "Si"]
    cdmasistencias = cdmasistencias.compute()
    print('Guardando archivo CDM_Asistencias_' + str(split))
    cdmasistencias.to_csv(path + r"\4-Salidas\1-Datos_por_ocurrencia\2-Asistencias\CDM_Asistencias_"+str(split)+".csv",sep=";",decimal=",",encoding="latin1", index=None)
    print('Archivo CDM_Asistencias_' + str(split), 'guardado\n')

    cdmsoat = cdmfin.loc[(cdmfin["cod_ramo"] == "11")]
    cdmsoat = cdmsoat.compute()
    print('Guardando archivo CDM_Soat_' + str(split))
    cdmsoat.to_csv(path + r"\4-Salidas\1-Datos_por_ocurrencia\4-Soat\CDM_Soat_"+str(split)+".csv",sep=";",decimal=",",encoding="latin1", index=None)
    print('Archivo CDM_Soat_' + str(split), 'guardado\n')

    cdmgyv = cdmfin.loc[((cdmfin["cod_ramo"] != "11") & (cdmfin["cod_ramo"] != "10") & (cdmfin["cod_ramo"] != "12")) & (cdmfin["es_asistencia"] == "No") ]
    cdmgyv = cdmgyv.compute()
    print('Guardando archivo CDM_Generales_y_Vida_' + str(split))
    cdmgyv.to_csv(path + r"\4-Salidas\1-Datos_por_ocurrencia\3-Generales_y_Vida\CDM_Generales_y_Vida_"+str(split)+".csv",sep=";",decimal=",",encoding="latin1", index=None)
    print('Archivo CDM_Generales_y_Vida_' + str(split), 'guardado\n')

    
    
    # Incrementar el año para la siguiente iteración
    split +=1
    

    finwh = datetime.strptime(time.strftime("%Y%m%d %I.%M.%S %p"),"%Y%m%d %I.%M.%S %p")
    tiempo_proceso_wh = finwh -iniciowh
    print("fin proceso "+str(split)+":" + time.strftime("%c"))
    print("Tiempo total de procesamiento"+str(split)+":\n" + str(tiempo_proceso_wh))



 
   
fin = datetime.strptime(time.strftime("%Y%m%d %I.%M.%S %p"),"%Y%m%d %I.%M.%S %p")
tiempo_proceso = fin -inicio
print("fin proceso: " + time.strftime("%c"))
print("Tiempo total de procesamiento:\n" + str(tiempo_proceso))
engine = pyttsx3.init()
engine.say('El proceso a finalizado ')
engine.runAndWait() 