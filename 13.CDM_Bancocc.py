# -*- coding: utf-8 -*-
"""
Created on Wed Mar  8 13:13:06 2023

@author: Edgaviriac
"""

import dask.dataframe as dd
import pandas as pd
from datetime import datetime
import time
import pyttsx3

path = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\21-CDM\DATOS\Formatos'

inicio = datetime.strptime(time.strftime("%Y%m%d %I.%M.%S %p"),"%Y%m%d %I.%M.%S %p")


print('Leyendo archivo: 1-Subsiniestros-Claims_Data_Manager-Autos_Gles_y_Vida')
claims = dd.read_csv(r"\\dc1pvfnas1\Autos\BusinessIntelligence\16-Claims_Data_Manager\1-Subsiniestros-Claims_Data_Manager-Autos_Gles_y_Vida.csv",sep=";",decimal=",",encoding="latin1",dtype="object")
print('Archivo: 1-Subsiniestros-Claims_Data_Manager-Autos_Gles_y_Vida leído \n')

print('Leyendo archivo: 2-Subsiniestros-Claims_Data_Manager-Asistencias')
asis = dd.read_csv(r"\\dc1pvfnas1\Autos\BusinessIntelligence\16-Claims_Data_Manager\2-Subsiniestros-Claims_Data_Manager-Asistencias.csv",sep=";",decimal=",",encoding="latin1",dtype="object")
genbocc = dd.read_csv(path + r"\BASE GENERAL BANCO DE OCCIDENTE.csv", sep=";",decimal=",",encoding="latin1",dtype="object")
encabezados = dd.read_csv(path + r"\Encabezados Bco Occidente.csv", sep=";",decimal=",",encoding="latin1",dtype="object")
participacion = dd.read_csv(path + r"\TABLA_PARTICIPACION_Banco_Occ.csv", sep=";",decimal=",",encoding="latin1")
antbocc = dd.read_csv(path + r"\Antiguo Banco de Occidente.csv", sep=";",decimal=",",encoding="latin1", dtype="object")

claims =claims.append(asis)

bancocc = claims[(claims["nro_doc"] == "8903002794")&(claims["cod_suc"]=="29")]

bancocc["Llave_Poliza"] = str(bancocc["nro_pol"])+"_"+bancocc["nom_ramo"]+"_"+bancocc["txt_suc_pol"] 
bancocc = bancocc.merge(participacion, how="left", on= "Llave_Poliza",suffixes=("","(2)"))
genbocc = genbocc.rename(columns={"SERIAL":"BIEN AFECTADO/SERIAL"})
antbocc = antbocc.rename(columns={"CARPETA DIGITAL":"CARPETA","ASEGURADO":"LOCATARIO"})
genbocc = genbocc.append(antbocc)
bancocc= bancocc.merge(genbocc, how="left", left_on="nro_carpeta_digital", right_on= "CARPETA" ,suffixes=("","(2)"))
bancocc = bancocc.drop_duplicates(subset="sub_key")
bancocc["BANCA"] ="Leasing"
bancocc["LOCATARIO"] = bancocc["LOCATARIO"].fillna("elim") 

bancocc["ASEGURADO"] = bancocc.apply(lambda row: row["Nombre asegurado"] if pd.isnull(row["LOCATARIO"]) else row["Nombre asegurado"],
                                     axis=1) 

bancocc["Valores.Rsva-Indem_Actual"]=bancocc["Valores.Rsva-Indem_Actual"].str.replace(",",".")
bancocc["Valores.Total Rsva_Actual"]=bancocc["Valores.Total Rsva_Actual"].str.replace(",",".")
bancocc["Valores.Pag-Indem_Final"]=bancocc["Valores.Pag-Indem_Final"].str.replace(",",".")
bancocc["Valores.Total Pagos_Final"]=bancocc["Valores.Total Pagos_Final"].str.replace(",",".")


bancocc =bancocc.compute()

bancocc["RESERVA INDEMNIZACION"] = bancocc["Valores.Rsva-Indem_Actual"].fillna("0").astype(float)/bancocc["Participación"].fillna(1.0)
bancocc["RESERVA TODO CONCEPTO"] = bancocc["Valores.Total Rsva_Actual"].fillna("0").astype(float)/bancocc["Participación"].fillna(1.0)
bancocc["PAGO INDEMNIZACION"] = bancocc["Valores.Pag-Indem_Final"].fillna("0").astype(float)/bancocc["Participación"].fillna(1.0)
bancocc["PAGO TODO CONCEPTO"] =  bancocc["Valores.Total Pagos_Final"].fillna("0").astype(float)/bancocc["Participación"].fillna(1.0)

print(bancocc["Participación"].dtype)

bancocc =bancocc.rename(columns={"txt_suc_pol":"SUCURSAL","nro_pol":"NRO POLIZA","nom_ramo":"RAMO-1","amparo":"RAMO-2","nro_stro":"NO. SINIESTRO","aaaa_ejercicio":"AÑO EJERCICIO",
                         "fec_ocurrido":"FECHA_SINIESTRO","fec_aviso":"FECHA_AVISO","Estado":"ESTADO","nro_carpeta_digital":"CARPETA DIGITAL"})

bancocc =bancocc[["BANCA","SUCURSAL","ASEGURADO","NRO POLIZA","CONTRATO","RAMO-1","RAMO-2","Ajustador","BIEN AFECTADO/SERIAL","NO. SINIESTRO","AÑO EJERCICIO","FECHA_SINIESTRO",
                  "FECHA_AVISO","RESERVA INDEMNIZACION","PAGO INDEMNIZACION","ESTADO","CARPETA DIGITAL","fec_vig_desde","fec_vig_hasta","First_Indemnizacion","1ra_Fec_Formalizado",
                 "Hechos","RESERVA TODO CONCEPTO","PAGO TODO CONCEPTO","Nombre Reclamante","Contacto","Numero Documento Contacto"]]

#bancocc = bancocc.compute()

bancocc = bancocc.reindex(columns=encabezados.columns)

bancocc.to_csv(r"\\DC1PVFNAS1\Autos\BusinessIntelligence\2-Gestión_Siniestros\2-AyM\BANCO DE OCCIDENTE\SINIESTRALIDAD BIENES INTERMEDIOS ord.csv",encoding="latin1",sep=";",decimal=",")







  
fin = datetime.strptime(time.strftime("%Y%m%d %I.%M.%S %p"),"%Y%m%d %I.%M.%S %p")
tiempo_proceso = fin -inicio
print("fin proceso: " + time.strftime("%c"))
print("Tiempo total de procesamiento:\n" + str(tiempo_proceso))
engine = pyttsx3.init()
engine.say('El proceso a finalizado ')
engine.runAndWait() 


    

