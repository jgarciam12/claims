# -*- coding: utf-8 -*-
"""
Created on Mon Mar  6 18:26:42 2023

@author: Edgaviriac
"""

import pandas as pd
import gc
import time
from datetime import datetime
import pyttsx3


inicio = datetime.strptime(time.strftime("%Y%m%d %I.%M.%S %p"),"%Y%m%d %I.%M.%S %p")
print(inicio)

path = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\21-CDM\DATOS\Formatos'
path2 = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\21-CDM\DATOS\4-Salidas\2-Consolidados'

campos= pd.read_csv(path + r"\Campos CDM.csv",sep=";",encoding="latin1",dtype="object",low_memory=False)
diccionario = campos.set_index("Campo")["tipo"].to_dict()


camp_am=campos.loc[campos["amparos"]!= "elim"]["Campo"]
camp_am_lt = campos.loc[campos["amparos"]== "ultimo"]["Campo"]
camp_am_ft = campos.loc[campos["amparos"]== "primero"]["Campo"] 
camp_am_sm = campos.loc[campos["amparos"]== "suma"]["Campo"]
camp_am_ct = campos.loc[campos["amparos"]== "concat"]["Campo"]
camp_am_mx = campos.loc[campos["amparos"]== "maximo"]["Campo"]


camp_stro=campos.loc[campos["siniestros"]!= "elim"]["Campo"]
camp_stro_lt = campos.loc[campos["siniestros"]== "ultimo"]["Campo"]
camp_stro_ft = campos.loc[campos["siniestros"]== "primero"]["Campo"] 
camp_stro_sm = campos.loc[campos["siniestros"]== "suma"]["Campo"]
camp_stro_ct = campos.loc[campos["siniestros"]== "concat"]["Campo"]
camp_stro_mx = campos.loc[campos["siniestros"]== "maximo"]["Campo"]


#########################################################################################################################################################################
############                                                          AUTOS, GENERALES Y VIDA                                                                  ##########
#########################################################################################################################################################################

agv = pd.read_csv(path2 + r"\1-Subsiniestros-Claims_Data_Manager-Autos_Gles_y_Vida.csv",sep=";",decimal=",",encoding="latin1",low_memory=False)

primeros_am = agv.groupby("amp_key")[camp_am_ft].first().reset_index()
ultimos_am = agv.groupby("amp_key")[camp_am_lt].last().reset_index()
sumas_am = agv.groupby("amp_key")[camp_am_sm].sum().reset_index()
concat_am = agv.groupby("amp_key")["Detalle_ Pagos"].apply(lambda x: "|".join(x.astype(str))).reset_index() 
concat_am_indem = agv.groupby("amp_key")["Detalle_indemnizacion"].apply(lambda x: "|".join(x.astype(str))).reset_index() 
maximos_am = agv.groupby("amp_key")[camp_am_mx].last().reset_index()


amparos = primeros_am.merge(ultimos_am, how="left", on="amp_key").merge(sumas_am, how="left", on="amp_key").merge(concat_am, how="left", on="amp_key").merge(maximos_am, how="left", on="amp_key").merge(concat_am_indem, how="left", on="amp_key")
amparos= amparos.reset_index()
amparos=amparos.drop_duplicates(subset="amp_key", keep="last")
amparos=amparos[camp_am]
print('Guardando archivo: 7-Amparos-Claims_Data_Manager-Autos_Gles_y_Vida')
amparos.to_csv(path2 + r"\7-Amparos-Claims_Data_Manager-Autos_Gles_y_Vida.csv",sep=";",decimal=",",encoding="latin1", index=None,)
print('Archivo 7-Amparos-Claims_Data_Manager-Autos_Gles_y_Vida guardado\n')


del primeros_am
del ultimos_am
del sumas_am
del concat_am
del maximos_am
del amparos
gc.collect()


primeros_stro = agv.groupby("sin_key")[camp_stro_ft].first().reset_index()
ultimos_stro = agv.groupby("sin_key")[camp_stro_lt].last().reset_index()
sumas_stro = agv.groupby("sin_key")[camp_stro_sm].sum().reset_index()
concat_stro = agv.groupby("sin_key")["Detalle_ Pagos"].apply(lambda x: "|".join(x.astype(str))).reset_index()
concat_stro_indem = agv.groupby("sin_key")["Detalle_indemnizacion"].apply(lambda x: "|".join(x.astype(str))).reset_index()
maximos_stro = agv.groupby("sin_key")[camp_stro_mx].last().reset_index()


siniestros = primeros_stro.merge(ultimos_stro, how="left", on="sin_key").merge(sumas_stro, how="left", on="sin_key").merge(concat_stro, how="left", on="sin_key").merge(maximos_stro, how="left", on="sin_key").merge(concat_stro_indem, how="left", on="sin_key")
siniestros = siniestros.reset_index()
siniestros = siniestros.drop_duplicates(subset="sin_key", keep="last")
siniestros = siniestros[camp_stro]
print('Guardando archivo: 13-Siniestros-Claims_Data_Manager-Autos_Gles_y_Vida')
siniestros.to_csv(path2 + r"\13-Siniestros-Claims_Data_Manager-Autos_Gles_y_Vida.csv",sep=";",decimal=",",encoding="latin1", index=None)
print('Archivo 13-Siniestros-Claims_Data_Manager-Autos_Gles_y_Vida guardado\n')
del primeros_stro
del ultimos_stro
del sumas_stro
del concat_stro
del maximos_stro
del siniestros
del agv
gc.collect()


#########################################################################################################################################################################
############                                                       ASISTENCIAS                                                                                 ##########
#########################################################################################################################################################################

asistencias = pd.read_csv(path2 + r"\2-Subsiniestros-Claims_Data_Manager-Asistencias.csv",sep=";",decimal=",",encoding="latin1",low_memory=False)


primeros_am = asistencias.groupby("amp_key")[camp_am_ft].first().reset_index()
ultimos_am = asistencias.groupby("amp_key")[camp_am_lt].last().reset_index()
sumas_am = asistencias.groupby("amp_key")[camp_am_sm].sum().reset_index()
concat_am = asistencias.groupby("amp_key")["Detalle_ Pagos"].apply(lambda x: "|".join(x.astype(str))).reset_index() 
concat_am_indem = asistencias.groupby("amp_key")["Detalle_indemnizacion"].apply(lambda x: "|".join(x.astype(str))).reset_index()
maximos_am = asistencias.groupby("amp_key")[camp_am_mx].last().reset_index()


amparos = primeros_am.merge(ultimos_am, how="left", on="amp_key").merge(sumas_am, how="left", on="amp_key").merge(concat_am, how="left", on="amp_key").merge(maximos_am, how="left", on="amp_key").merge(concat_am_indem, how="left", on="amp_key")
amparos= amparos.reset_index()
amparos=amparos.drop_duplicates(subset="amp_key", keep="last")
amparos=amparos[camp_am]
print('Guardando archivo: 8-Amparos-Claims_Data_Manager-Asistencias')
amparos.to_csv(path2 + r"\8-Amparos-Claims_Data_Manager-Asistencias.csv",sep=";",decimal=",",encoding="latin1", index=None,)
print('Archivo 8-Amparos-Claims_Data_Manager-Asistencias guardado\n')


del primeros_am
del ultimos_am
del sumas_am
del concat_am
del maximos_am
del amparos
gc.collect()


primeros_stro = asistencias.groupby("sin_key")[camp_stro_ft].first().reset_index()
ultimos_stro = asistencias.groupby("sin_key")[camp_stro_lt].last().reset_index()
sumas_stro = asistencias.groupby("sin_key")[camp_stro_sm].sum().reset_index()
concat_stro = asistencias.groupby("sin_key")["Detalle_ Pagos"].apply(lambda x: "|".join(x.astype(str))).reset_index()
concat_stro_indem = asistencias.groupby("sin_key")["Detalle_indemnizacion"].apply(lambda x: "|".join(x.astype(str))).reset_index()
maximos_stro = asistencias.groupby("sin_key")[camp_stro_mx].last().reset_index()


siniestros = primeros_stro.merge(ultimos_stro, how="left", on="sin_key").merge(sumas_stro, how="left", on="sin_key").merge(concat_stro, how="left", on="sin_key").merge(maximos_stro, how="left", on="sin_key").merge(concat_stro_indem, how="left", on="sin_key")
siniestros = siniestros.reset_index()
siniestros = siniestros.drop_duplicates(subset="sin_key", keep="last")
siniestros = siniestros[camp_stro]
print('Guardando archivo: 14-Siniestros-Claims_Data_Manager-Asistencias')
siniestros.to_csv(path2 + r"\14-Siniestros-Claims_Data_Manager-Asistencias.csv",sep=";",decimal=",",encoding="latin1", index=None)
print('Archivo 14-Siniestros-Claims_Data_Manager-Asistencias guardado\n')

del primeros_stro
del ultimos_stro
del sumas_stro
del concat_stro
del maximos_stro
del siniestros
del asistencias
gc.collect()




#########################################################################################################################################################################
############                                                                    SOAT                                                                           ##########
#########################################################################################################################################################################



soat = pd.read_csv(path2 + r"\3-Subsiniestros-Claims_Data_Manager-Soat.csv",sep=";",decimal=",",encoding="latin1",low_memory=False)


primeros_am = soat.groupby("amp_key")[camp_am_ft].first().reset_index()
ultimos_am = soat.groupby("amp_key")[camp_am_lt].last().reset_index()
sumas_am = soat.groupby("amp_key")[camp_am_sm].sum().reset_index()
concat_am = soat.groupby("amp_key")["Detalle_ Pagos"].apply(lambda x: "|".join(x.astype(str))).reset_index() 
concat_am_indem = soat.groupby("amp_key")["Detalle_indemnizacion"].apply(lambda x: "|".join(x.astype(str))).reset_index()
maximos_am = soat.groupby("amp_key")[camp_am_mx].last().reset_index()


amparos = primeros_am.merge(ultimos_am, how="left", on="amp_key").merge(sumas_am, how="left", on="amp_key").merge(concat_am, how="left", on="amp_key").merge(maximos_am, how="left", on="amp_key").merge(concat_am_indem, how="left", on="amp_key")
amparos= amparos.reset_index()
amparos=amparos.drop_duplicates(subset="amp_key", keep="last")
amparos=amparos[camp_am]
print('Guardando archivo: 9-Amparos-Claims_Data_Manager-Soat')
amparos.to_csv(path2 + r"\9-Amparos-Claims_Data_Manager-Soat.csv",sep=";",decimal=",",encoding="latin1", index=None,)
print('Archivo 9-Amparos-Claims_Data_Manager-Soat guardado\n')


del primeros_am
del ultimos_am
del sumas_am
del concat_am
del maximos_am
del amparos
gc.collect()


primeros_stro = soat.groupby("sin_key")[camp_stro_ft].first().reset_index()
ultimos_stro = soat.groupby("sin_key")[camp_stro_lt].last().reset_index()
sumas_stro = soat.groupby("sin_key")[camp_stro_sm].sum().reset_index()
concat_stro = soat.groupby("sin_key")["Detalle_ Pagos"].apply(lambda x: "|".join(x.astype(str))).reset_index()
concat_stro_indem = soat.groupby("sin_key")["Detalle_indemnizacion"].apply(lambda x: "|".join(x.astype(str))).reset_index()
maximos_stro = soat.groupby("sin_key")[camp_stro_mx].last().reset_index()


siniestros = primeros_stro.merge(ultimos_stro, how="left", on="sin_key").merge(sumas_stro, how="left", on="sin_key").merge(concat_stro, how="left", on="sin_key").merge(maximos_stro, how="left", on="sin_key").merge(concat_stro_indem, how="left", on="sin_key")
siniestros = siniestros.reset_index()
siniestros = siniestros.drop_duplicates(subset="sin_key", keep="last")
siniestros = siniestros[camp_stro]
print('Guardando archivo: 15-Siniestros-Claims_Data_Manager-Soat')
siniestros.to_csv(path2 + r"\15-Siniestros-Claims_Data_Manager-Soat.csv",sep=";",decimal=",",encoding="latin1", index=None,)
print('Archivo 15-Siniestros-Claims_Data_Manager-Soat\n')

del primeros_stro
del ultimos_stro
del sumas_stro
del concat_stro
del maximos_stro
del siniestros
del soat
gc.collect()





#########################################################################################################################################################################
############                                                SOLO GENERALES Y VIDA                                                                              ##########
#########################################################################################################################################################################

gyv = pd.read_csv(path2 + r"\4-Subsiniestros-Claims_Data_Manager-Solo_Gles_y_Vida.csv",sep=";",decimal=",",encoding="latin1",low_memory=False)

primeros_am = gyv.groupby("amp_key")[camp_am_ft].first().reset_index()
ultimos_am = gyv.groupby("amp_key")[camp_am_lt].last().reset_index()
sumas_am = gyv.groupby("amp_key")[camp_am_sm].sum().reset_index()
concat_am = gyv.groupby("amp_key")["Detalle_ Pagos"].apply(lambda x: "|".join(x.astype(str))).reset_index() 
concat_am_indem = gyv.groupby("amp_key")["Detalle_indemnizacion"].apply(lambda x: "|".join(x.astype(str))).reset_index()
maximos_am = gyv.groupby("amp_key")[camp_am_mx].last().reset_index()


amparos = primeros_am.merge(ultimos_am, how="left", on="amp_key").merge(sumas_am, how="left", on="amp_key").merge(concat_am, how="left", on="amp_key").merge(maximos_am, how="left", on="amp_key").merge(concat_am_indem, how="left", on="amp_key")
amparos= amparos.reset_index()
amparos=amparos.drop_duplicates(subset="amp_key", keep="last")
amparos=amparos[camp_am]
#%%
print('Guardando archivo: 10-Amparos-Claims_Data_Manager-Solo_Gles_y_Vida')
amparos.to_csv(path2 + r"\10-Amparos-Claims_Data_Manager-Solo_Gles_y_Vida.csv",sep=";",decimal=",",encoding="latin1", index=None)
print('Archivo 10-Amparos-Claims_Data_Manager-Solo_Gles_y_Vida guardado\n')


del primeros_am
del ultimos_am
del sumas_am
del concat_am
del maximos_am
del amparos
gc.collect()


primeros_stro = gyv.groupby("sin_key")[camp_stro_ft].first().reset_index()
ultimos_stro = gyv.groupby("sin_key")[camp_stro_lt].last().reset_index()
sumas_stro = gyv.groupby("sin_key")[camp_stro_sm].sum().reset_index()
concat_stro = gyv.groupby("sin_key")["Detalle_ Pagos"].apply(lambda x: "|".join(x.astype(str))).reset_index()
concat_stro_indem = gyv.groupby("sin_key")["Detalle_indemnizacion"].apply(lambda x: "|".join(x.astype(str))).reset_index()
maximos_stro = gyv.groupby("sin_key")[camp_stro_mx].last().reset_index()


siniestros = primeros_stro.merge(ultimos_stro, how="left", on="sin_key").merge(sumas_stro, how="left", on="sin_key").merge(concat_stro, how="left", on="sin_key").merge(maximos_stro, how="left", on="sin_key").merge(concat_stro_indem, how="left", on="sin_key")
siniestros = siniestros.reset_index()
siniestros = siniestros.drop_duplicates(subset="sin_key", keep="last")
siniestros = siniestros[camp_stro]
print('Guardando archivo: 16-Siniestros-Claims_Data_Manager-Solo_Gles_y_Vida')
siniestros.to_csv(path2 + r"\16-Siniestros-Claims_Data_Manager-Solo_Gles_y_Vida.csv",sep=";",decimal=",",encoding="latin1", index=None)
print('Archivo 16-Siniestros-Claims_Data_Manager-Solo_Gles_y_Vida guardado\n')

del primeros_stro
del ultimos_stro
del sumas_stro
del concat_stro
del maximos_stro
del siniestros
del gyv
gc.collect()





#########################################################################################################################################################################
############                                                                SOLO AUTOS                                                                         ##########
#########################################################################################################################################################################

solo_au = pd.read_csv(path2 + r"\5-Subsiniestros-Claims_Data_Manager-Solo_Autos.csv",sep=";",decimal=",",encoding="latin1",low_memory=False)

primeros_am = solo_au.groupby("amp_key")[camp_am_ft].first().reset_index()
ultimos_am = solo_au.groupby("amp_key")[camp_am_lt].last().reset_index()
sumas_am = solo_au.groupby("amp_key")[camp_am_sm].sum().reset_index()
concat_am = solo_au.groupby("amp_key")["Detalle_ Pagos"].apply(lambda x: "|".join(x.astype(str))).reset_index() 
concat_am_indem = solo_au.groupby("amp_key")["Detalle_indemnizacion"].apply(lambda x: "|".join(x.astype(str))).reset_index()
maximos_am = solo_au.groupby("amp_key")[camp_am_mx].last().reset_index()


amparos = primeros_am.merge(ultimos_am, how="left", on="amp_key").merge(sumas_am, how="left", on="amp_key").merge(concat_am, how="left", on="amp_key").merge(maximos_am, how="left", on="amp_key").merge(concat_am_indem, how="left", on="amp_key")
amparos= amparos.reset_index()
amparos=amparos.drop_duplicates(subset="amp_key", keep="last")
amparos=amparos[camp_am]
print('Guardando archivo: 11-Amparos-Claims_Data_Manager-Solo_Autos')
amparos.to_csv(path2 + r"\11-Amparos-Claims_Data_Manager-Solo_Autos.csv",sep=";",decimal=",",encoding="latin1", index=None)
print('Archivo 11-Amparos-Claims_Data_Manager-Solo_Autos guardado\n')


del primeros_am
del ultimos_am
del sumas_am
del concat_am
del maximos_am
del amparos
gc.collect()


primeros_stro = solo_au.groupby("sin_key")[camp_stro_ft].first().reset_index()
ultimos_stro = solo_au.groupby("sin_key")[camp_stro_lt].last().reset_index()
sumas_stro = solo_au.groupby("sin_key")[camp_stro_sm].sum().reset_index()
concat_stro = solo_au.groupby("sin_key")["Detalle_ Pagos"].apply(lambda x: "|".join(x.astype(str))).reset_index()
concat_stro_indem = solo_au.groupby("sin_key")["Detalle_indemnizacion"].apply(lambda x: "|".join(x.astype(str))).reset_index()
maximos_stro = solo_au.groupby("sin_key")[camp_stro_mx].last().reset_index()


siniestros = primeros_stro.merge(ultimos_stro, how="left", on="sin_key").merge(sumas_stro, how="left", on="sin_key").merge(concat_stro, how="left", on="sin_key").merge(maximos_stro, how="left", on="sin_key").merge(concat_stro_indem, how="left", on="sin_key")
siniestros = siniestros.reset_index()
siniestros = siniestros.drop_duplicates(subset="sin_key", keep="last")
siniestros = siniestros[camp_stro]
print('Guardando archivo: 17-Siniestros-Claims_Data_Manager-Solo_Autos')
siniestros.to_csv(path2 + r"\17-Siniestros-Claims_Data_Manager-Solo_Autos.csv",sep=";",decimal=",",encoding="latin1", index=None)
print('Archivo 17-Siniestros-Claims_Data_Manager-Solo_Autos guardado\n')

del primeros_stro
del ultimos_stro
del sumas_stro
del concat_stro
del maximos_stro
del siniestros
del solo_au
gc.collect()




#########################################################################################################################################################################
############                                                AUTOS CON ASISTENCIAS                                                                              ##########
#########################################################################################################################################################################

autos_asis = pd.read_csv(path2 + r"\6-Subsiniestros-Claims_Data_Manager-Autos_con_Asistencias.csv",sep=";",decimal=",",encoding="latin1",low_memory=False)

primeros_am = autos_asis.groupby("amp_key")[camp_am_ft].first().reset_index()
ultimos_am = autos_asis.groupby("amp_key")[camp_am_lt].last().reset_index()
sumas_am = autos_asis.groupby("amp_key")[camp_am_sm].sum().reset_index()
concat_am = autos_asis.groupby("amp_key")["Detalle_ Pagos"].apply(lambda x: "|".join(x.astype(str))).reset_index() 
concat_am_indem = autos_asis.groupby("amp_key")["Detalle_indemnizacion"].apply(lambda x: "|".join(x.astype(str))).reset_index()
maximos_am = autos_asis.groupby("amp_key")[camp_am_mx].last().reset_index()


amparos = primeros_am.merge(ultimos_am, how="left", on="amp_key").merge(sumas_am, how="left", on="amp_key").merge(concat_am, how="left", on="amp_key").merge(maximos_am, how="left", on="amp_key").merge(concat_am_indem, how="left", on="amp_key")
amparos= amparos.reset_index()
amparos=amparos.drop_duplicates(subset="amp_key", keep="last")
amparos=amparos[camp_am]
print('Guardando archivo: 12-Amparos-Claims_Data_Manager-Autos_con_Asistencias')
amparos.to_csv(path2 + r"\12-Amparos-Claims_Data_Manager-Autos_con_Asistencias.csv",sep=";",decimal=",",encoding="latin1", index=None)
print('Archivo 12-Amparos-Claims_Data_Manager-Autos_con_Asistencias guardado\n')


del primeros_am
del ultimos_am
del sumas_am
del concat_am
del maximos_am
del amparos
gc.collect()


primeros_stro = autos_asis.groupby("sin_key")[camp_stro_ft].first().reset_index()
ultimos_stro = autos_asis.groupby("sin_key")[camp_stro_lt].last().reset_index()
sumas_stro = autos_asis.groupby("sin_key")[camp_stro_sm].sum().reset_index()
concat_stro = autos_asis.groupby("sin_key")["Detalle_ Pagos"].apply(lambda x: "|".join(x.astype(str))).reset_index()
concat_stro_indem = autos_asis.groupby("sin_key")["Detalle_indemnizacion"].apply(lambda x: "|".join(x.astype(str))).reset_index()
maximos_stro = autos_asis.groupby("sin_key")[camp_stro_mx].last().reset_index()


siniestros = primeros_stro.merge(ultimos_stro, how="left", on="sin_key").merge(sumas_stro, how="left", on="sin_key").merge(concat_stro, how="left", on="sin_key").merge(maximos_stro, how="left", on="sin_key").merge(concat_stro_indem, how="left", on="sin_key")
siniestros = siniestros.reset_index()
siniestros = siniestros.drop_duplicates(subset="sin_key", keep="last")
siniestros = siniestros[camp_stro]
print('Guardando archivo: 18-Siniestros-Claims_Data_Manager-Autos_con_Asistencias')
siniestros.to_csv(path2 + r"\18-Siniestros-Claims_Data_Manager-Autos_con_Asistencias.csv",sep=";",decimal=",",encoding="latin1", index=None)
print('Archivo 18-Siniestros-Claims_Data_Manager-Autos_con_Asistencias guardado\n')

del primeros_stro
del ultimos_stro
del sumas_stro
del concat_stro
del maximos_stro
del siniestros
del autos_asis
gc.collect()




fin = datetime.strptime(time.strftime("%Y%m%d %I.%M.%S %p"),"%Y%m%d %I.%M.%S %p")
tiempo_proceso = fin -inicio
print("fin proceso: " + time.strftime("%c"))
print("Tiempo total de procesamiento:\n" + str(tiempo_proceso))
engine = pyttsx3.init()
engine.say('El proceso a finalizado ')
engine.runAndWait()























