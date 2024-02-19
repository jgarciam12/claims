# -*- coding: utf-8 -*-
"""
Created on Sat Feb 11 13:39:13 2023

@author: Edgaviriac
"""

####REPORTEADOR MENSUAL####



import dask.dataframe as dd
import gc
import time
import pyttsx3
from datetime import datetime
import pandas as pd

path = r'\\dc1pvfnas1\Autos\BusinessIntelligence\21-CDM\DATOS'

def dectx_toint(row):
    return row.fillna("0,0").str.replace(",",".").astype(float).astype(int)

inicio = datetime.strptime(time.strftime("%Y%m%d %I.%M.%S %p"),"%Y%m%d %I.%M.%S %p")
print('Inicio: ',str(inicio), '\n')
#%%
print('Leyendo archivo Columnas_intermediarios')
columns_names_generic = dd.read_csv(path + r"\Formatos\Columnas_intermediarios.csv",encoding = "ANSI",sep=";",lineterminator=("\r\n"),quotechar='"',na_values=["","NA"],dayfirst=(True),low_memory=(False),decimal=",",dtype = "object")    
print('Archivo Columnas_intermediarios leido \n')

print('Leyendo archivo Columnas_merck')
columns_names_merck = dd.read_csv(path + r"\Formatos\Columnas_merck.csv",encoding = "ANSI",sep=";",lineterminator=("\r\n"),quotechar='"',na_values=["","NA"],dayfirst=(True),low_memory=(False),decimal=",",dtype = "object")    
print('Archivo Columnas_merck leido \n')

print('Leyendo archivo Columnas_intermediarios_aym')
columns_names_aym = dd.read_csv(path + r"\Formatos\Columnas_intermediarios_aym.csv",encoding = "ANSI",sep=";",lineterminator=("\r\n"),quotechar='"',na_values=["","NA"],dayfirst=(True),low_memory=(False),decimal=",",dtype = "object")    
print('Archivo Columnas_intermediarios_aym leido \n')

print('Leyendo archivo Intermediarios a notificar')
intermediarios = dd.read_csv(path + r"\Formatos\Intermediarios a notificar.csv",encoding = "ANSI",sep=";",lineterminator=("\r\n"),quotechar='"',na_values=["","NA"],dayfirst=(True),low_memory=(False),decimal=",",dtype = "object")    
print('Archivo Intermediarios a notificar leido \n')

print('Leyendo archivo Intermediarios a notificar AYM')
aym = dd.read_csv(path + r"\Formatos\Intermediarios a notificar AYM.csv",encoding = "ANSI",sep=";",lineterminator=("\r\n"),quotechar='"',na_values=["","NA"],dayfirst=(True),low_memory=(False),decimal=",",dtype = "object",usecols=["cuenta_intermediario_aym","agente_ramo"])
print('Archivo Intermediarios a notificar AYM leido \n')

print('Leyendo archivo Nits a notificar')
nits = dd.read_csv(path + r"\Formatos\Nits a notificar.csv",encoding = "ANSI",sep=";",lineterminator=("\r\n"),quotechar='"',na_values=["","NA"],dayfirst=(True),low_memory=(False),decimal=",",dtype = "object")    
print('Archivo Nits a notificar leido \n')

#smart = dd.read_csv(r"\\dc1pvfnas1\Autos\BusinessIntelligence\17-ModelosExternos\5-Sistema de Monitoreo PP Automoviles\0-Salidas\1-Datos\1-Datos_principales_smart.csv",encoding = "ANSI",sep=";",lineterminator=("\r\n"),quotechar='"',na_values=["","NA"],dayfirst=(True),low_memory=(False),decimal=",",dtype = "object")  
print('Leyendo archivo 1-Datos_principales_smart')
smart = pd.read_csv(r"\\dc1pvfnas1\Autos\BusinessIntelligence\17-ModelosExternos\5-Sistema de Monitoreo PP Automoviles\0-Salidas\1-Datos\1-Datos_principales_smart.csv",encoding = "ANSI",sep=";",quotechar='"',na_values=["","NA"],dayfirst=(True),low_memory=(False),decimal=",",dtype = "object")
print('Archivo 1-Datos_principales_smart leido \n')

intermediarios = intermediarios.compute()
nits = nits.compute()
aym = aym.compute()

amp_gyv=dd.read_csv(path + r"\4-Salidas\2-Consolidados\7-Amparos-Claims_Data_Manager-Autos_Gles_y_Vida.csv",encoding = "ANSI",sep=";",lineterminator=("\r\n"),quotechar='"',na_values=["","NA"],dayfirst=(True),low_memory=(False),decimal=",",dtype = "object")    
amp_asis=dd.read_csv(path + r"\4-Salidas\2-Consolidados\8-Amparos-Claims_Data_Manager-Asistencias.csv",encoding = "ANSI",sep=";",lineterminator=("\r\n"),quotechar='"',na_values=["","NA"],dayfirst=(True),low_memory=(False),decimal=",",dtype = "object")    
amp_complete=amp_gyv.append(amp_asis)

amp_complete["cod_agente"] = dectx_toint(amp_complete["cod_agente"]).astype(str)
amp_complete["cod_ramo"] = dectx_toint(amp_complete["cod_ramo"]).astype(str)
amp_complete["nro_stro"] = dectx_toint(amp_complete["nro_stro"]).astype(str)
amp_complete["cod_suc"] = dectx_toint(amp_complete["cod_suc"]).astype(str)
amp_complete["aaaa_ejercicio"] = dectx_toint(amp_complete["aaaa_ejercicio"]).astype(str)


amp_complete = amp_complete.compute()

del amp_asis
del amp_gyv
gc.collect()




#------------ Informe Merck ---------------------------------------------------------------------------------------


#amp_complete["cod_agrup"] = amp_complete["cod_agrup"].str.replace(",",".")

merck = amp_complete[dectx_toint(amp_complete["cod_agrup"]).isin([7869,7868,9415,9126])]

merck["Año Expedición"] = dd.to_datetime(merck["fec_vig_desde"], dayfirst=True).dt.year
merck["Pais_"]="Colombia"

def estadorsvaind_act(valor): 
    if valor > 0: 
        return "Open" 
    else: 
        return "Closed" 
    

    
merck["Valores.Rsva-Indem_Actual"] = dectx_toint(merck["Valores.Rsva-Indem_Actual"])
merck["Status"] = merck["Valores.Rsva-Indem_Actual"].apply(estadorsvaind_act, )#meta=("Status", "f8"))
def perdida_total(amparo):
    if "total" in amparo:
        return "Yes"
    else:
        return "No"
merck['Perdida Total'] = merck['amparo'].apply(perdida_total, )#meta=('Perdida Total', 'f8'))
merck["Moneda"] = "COP"

merck = merck[columns_names_merck.columns]
merck.to_csv(path + r"\4-Salidas\3-Masivos\1-Segmentados_Intermediarios_y_Corredores\Siniestralidad por amparos MERCK.csv",
               #sheet_name="Data", index=None,)
                encoding = "ANSI",sep=";",decimal=",", index=None)

del merck
gc.collect()

#-------------------------------------------------------------------------------------------------------------------

#------------ Informe Cencosud ---------------------------------------------------------------------------------------


cencosud = amp_complete.dropna(subset=['nom_aseg', 'agente_interm'])
cencosud = cencosud[(cencosud['nom_aseg'].astype(str).str.upper().str.contains('CENCOSUD') == True) | (cencosud['agente_interm'].astype(str).str.upper().str.contains('CENCOSUD') == True)]

cencosud = cencosud[columns_names_generic.columns]
cencosud.to_csv(path + r"\4-Salidas\3-Masivos\1-Segmentados_Intermediarios_y_Corredores\Siniestralidad por amparos CENCOSUD.csv",
        encoding = "ANSI",sep=";",decimal=",",index=None)

del cencosud
gc.collect()
#-------------------------------------------------------------------------------------------------------------------


#------------ Informe Porsche ---------------------------------------------------------------------------------------


porsche = amp_complete
porsche_au = porsche[dectx_toint(porsche['cod_agente'])==58900] #(porsche['nom_ramo']=='AUTOMOVILES')&
porsche_vd = porsche[(porsche['nom_ramo']!='AUTOMOVILES') & (porsche['cod_agente'].astype(int)==58900)]

print(porsche["cod_agente"].dtype)

porsche = porsche[columns_names_generic.columns]
porsche_au.to_csv(path + r"\4-Salidas\3-Masivos\1-Segmentados_Intermediarios_y_Corredores\Siniestralidad por amparos PORSCHE AUTOS.csv",
        encoding = "ANSI",sep=";",decimal=",",index=None)

porsche_vd.to_csv(path + r"\4-Salidas\3-Masivos\1-Segmentados_Intermediarios_y_Corredores\Siniestralidad por amparos PORSCHE VIDA.csv",
        encoding = "ANSI",sep=";",decimal=",",index=None)

del porsche_au
del porsche_vd
gc.collect()

#-------------------------------------------------------------------------------------------------------------------





#######################################################################################################################################################################################
#####################################################################               MASIVOS             ###############################################################################
#######################################################################################################################################################################################
#%%                            

amp_complete["llave_sipo"] = "No Aplica"
smart = smart[["Número de Expediente","Nombre del taller","Ciudad Taller"]]
#smart = smart.compute()
amp_complete["llave_sipo"] = amp_complete.apply(lambda row: 
                                                row["llave_sipo"] if row["nom_ramo"] !="AUTOMOVILES" 
                                                else 
                                                str(row["nro_stro"]) + "-" + str(row["cod_suc"]) +"-"+ str(row["aaaa_ejercicio"])
                                                ,axis=1)
amp_complete = amp_complete.merge(smart,how="left",left_on="llave_sipo",right_on="Número de Expediente")
#amp_complete = amp_complete[columns_names_generic.columns]
amp_complete["llave_aym"]= amp_complete["cod_agente"].astype(str) +"_" + amp_complete["nom_ramo"].astype(str)
amp_complete= dd.merge(amp_complete,intermediarios,how="left",left_on="cod_agente",right_on="Codigo Agente")
amp_complete= dd.merge(amp_complete,nits,how="left",left_on="nro_doc",right_on="Nit")
amp_complete = dd.merge(amp_complete,aym,how="left",left_on="llave_aym",right_on="agente_ramo")
amp_complete["ruta_int"] = path + r"\4-Salidas\3-Masivos\1-Segmentados_Intermediarios_y_Corredores\Siniestralidad por amparos " + amp_complete["cuenta_intermediario"] + ".csv"
amp_complete["ruta_nit"] = path + r"\4-Salidas\3-Masivos\1-Segmentados_Intermediarios_y_Corredores\Siniestralidad por amparos " + amp_complete["cuenta_documento"] + ".csv"
amp_complete["ruta_aym"] = path + r"\4-Salidas\3-Masivos\1-Segmentados_Intermediarios_y_Corredores\Siniestralidad por amparos " + amp_complete["cuenta_intermediario_aym"] + ".csv"

ruta = amp_complete[["ruta_int"]]
ruta2 = amp_complete[["ruta_nit"]]
ruta3 = amp_complete[["ruta_aym"]]
ruta = ruta.drop_duplicates().dropna()
ruta2 = ruta2.drop_duplicates().dropna()
ruta3 = ruta3.drop_duplicates().dropna()
ruta = ruta["ruta_int"].tolist()
ruta2= ruta2["ruta_nit"].tolist()
ruta3 = ruta3["ruta_aym"].tolist()

###################    MASIVOS POR CODIGO AGENTE    

for guardar in ruta:
    
    indiv = amp_complete[amp_complete["ruta_int"]==guardar]
    indiv.drop(columns=["ruta_int","ruta_nit","ruta_aym","cuenta_intermediario","Codigo Agente","Nit","cuenta_documento"])
    indiv = indiv[columns_names_generic.columns]
    indiv.to_csv(guardar,
            encoding = "ANSI",sep=";",decimal=",",index=None)
    del indiv
    gc.collect()



###################    MASIVOS POR CODIGO AGENTE Y RAMO     (ALIANZAS Y MASIVOS)

                                

for guardar3 in ruta3:
    
    indiv = amp_complete[amp_complete["ruta_aym"]==guardar3]
    indiv.drop(columns=["ruta_int","ruta_nit","ruta_aym","cuenta_intermediario","Codigo Agente","Nit","cuenta_documento"])
    indiv = indiv[columns_names_aym.columns]
    indiv.to_csv(guardar3,
            encoding = "ANSI",sep=";",decimal=",",index=None)
    del indiv
    gc.collect()
    
###################    MASIVOS POR NIT    
    
for guardar2 in ruta2:
    
    indiv = amp_complete[amp_complete["ruta_nit"]==guardar2]
    indiv.drop(columns=["ruta_int","ruta_nit","ruta_aym","cuenta_intermediario","Codigo Agente","Nit","cuenta_documento"])
    indiv = indiv[columns_names_generic.columns]
    indiv.to_csv(guardar2,
            encoding = "ANSI",sep=";",decimal=",",index=None)
    del indiv
    gc.collect()





fin = datetime.strptime(time.strftime("%Y%m%d %I.%M.%S %p"),"%Y%m%d %I.%M.%S %p")
tiempo_proceso = fin -inicio
print("fin proceso: " + time.strftime("%c"))
print("Tiempo total de procesamiento:\n" + str(tiempo_proceso))
engine = pyttsx3.init()
engine.say('El proceso a finalizado ')
engine.runAndWait() 



