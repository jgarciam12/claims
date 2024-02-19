# -*- coding: utf-8 -*-
"""
Created on Thu Mar  2 08:36:55 2023

@author: Edgaviriac
"""

# Definir los años de inicio y final para el bucle
split = 2015
final = 2024

import dask.dataframe as dd
import dask.array as da
import pandas as pd
import time
import pyttsx3
from datetime import datetime
from dateutil.relativedelta import relativedelta
import re
import gc
#%%
iniciobuc = datetime.strptime(time.strftime("%Y%m%d %I.%M.%S %p"),"%Y%m%d %I.%M.%S %p")
# Definir la ruta de entrada de los archivos
path_entrada = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\21-CDM\DATOS'

# Bucle principal para cada año
while split <= final:
    # Obtener la hora de inicio para el año actual
    print(split)
    inicio = datetime.strptime(time.strftime("%Y%m%d %I.%M.%S %p"),"%Y%m%d %I.%M.%S %p")
    hoy = inicio.date()
    retroc = hoy.replace(day=1) - relativedelta(days=1)
    fec_cierre = retroc.replace(day=27)
    fec_alerta = (hoy - relativedelta(years=1)).replace(day=27)
    fec_param = (fec_alerta + relativedelta(months=5)).replace(day=27)
    
    # Leer los archivos de entrada
    cierre = dd.read_parquet(path_entrada + r"\Por año siniestro\Mixxed_" + str(split) + ".parquet")
    #cierre = dd.read_parquet(r"D:\Fuentes\Detallado Historico\arvhivo_autos_stros.parquet")
    multi = dd.read_csv(path_entrada + r"\Reporte Multiple de Reservas Pendientes.csv", sep=";", encoding="latin1",low_memory=False,lineterminator=("\r\n"), dtype = "object")
    param = dd.read_csv(path_entrada + r"\Formatos\Tabla Reservas Paramétricas.csv",sep=";",encoding="latin1",lineterminator=("\r\n"),usecols=["LLAVE","Es_Param"])
    
    codigos_bases = [["Generales",1],["Vida",0]]
    codigos_bases= da.from_array(codigos_bases,chunks=(2,2))
    codigos_bases = dd.from_dask_array(codigos_bases,columns=("Base","Cod_Base"))
    multi = multi.merge(codigos_bases,left_on="Base",right_on="Base",how="left") 
    multi["sub_key(mr)"] = multi["cod_ramo"].map(str) + "_" + multi["cod_ramo_tecnico"].map(str) + "_" + multi["cod_suc"].map(str) + "_" +multi["nro_stro"].map(str) +"_" + multi["cod_ind_cob"].map(str) + "_" + multi["cod_tercero"].map(str) + "_" + multi["cod_item"].map(str) +"-"+ multi["Cod_Base"].map(str) # Se crea llave de subsiniestro
    # Convertir las columnas en objetos de fecha y hora de Pandas 
    multi["fec_ult_modif_rva"] = dd.to_datetime(multi["fec_ult_modif_rva"], dayfirst=True) 
    multi["fec_actualiza_stro"] = dd.to_datetime(multi["fec_actualiza_stro"], dayfirst=True)
    multi = multi.map_partitions(lambda partition: partition.assign(Ultima_modif=lambda x: x.apply(lambda row: max(pd.to_datetime(row['fec_ult_modif_rva']), pd.to_datetime(row['fec_actualiza_stro'])), axis=1)))
    multi = multi.groupby("sub_key(mr)")["Ultima_modif"].max().reset_index()
   
    
    

    # Lectura del archivo Campos CDM.csv y selección de campos de origen "Athento"
    camposcdm =  dd.read_csv(path_entrada + r"\Formatos\Campos CDM.csv", sep=";",encoding="latin1")
    incluirath = camposcdm["Campo"][camposcdm["Origen"]=="Athento"]
    # Lectura de archivos CSV relacionados con Athento y otros datos
    # Utilización de "incluirath" para seleccionar las columnas relevantes
    athento = dd.read_csv(r"\\dc1pvfnas1\Autos\BusinessIntelligence\4-Origenes\1-Generales_y_Vida\2-Complementarios\reportesiniestros.csv", sep=";",encoding="latin1", usecols = incluirath,dtype="object" )
    athento2 = dd.read_csv(r"\\dc1pvfnas1\Autos\BusinessIntelligence\4-Origenes\1-Generales_y_Vida\2-Complementarios\siniestros_athento_se.csv", sep=";",encoding="latin1", dtype="object" )
    # Eliminación de duplicados en el DataFrame "lesiones" basado en la columna "llave ods"
    lesiones =  dd.read_csv(path_entrada + r"\Formatos\Historico Lesiones.csv", sep=";",encoding="latin1")
    graves = dd.read_csv(path_entrada + r"\Formatos\Montos Graves.csv", sep=";",encoding="latin1")
    amp_autos = dd.read_csv(path_entrada + r"\Formatos\Tabla Amparos.csv", sep=";",encoding="latin1",dtype="object")
    base_destino = dd.read_csv(path_entrada + "\Formatos\Base Destino.csv", sep=";",encoding="latin1",dtype="object")
    reasignados = dd.read_csv(path_entrada + r"\Formatos\Reasignados.csv", sep=";",encoding="latin1",dtype="object")
    claves_agentes = dd.read_csv(path_entrada + r"\Formatos\Base Intermediarios.csv", sep=";",encoding="latin1",dtype="object",usecols=["cod_agente","nom_agente_verif"])
    
    lesiones.drop_duplicates(subset=["llave ods"],keep="last")

    
    # Combinación de datos relacionados con Athento y otras fuentes
    athento = athento.append(athento2)
    athento = athento.drop_duplicates(subset=["NumeroRadicadoAthento"], keep="last")

    # Combinación y manipulación de varios DataFrames para generar el DataFrame "cdm"
    cdm = cierre.merge(graves, how="left", left_on="nom_ramo", right_on="ramo")
    cdm = cdm.merge(amp_autos, how="left", on="cod_ind_cob")
    cdm = cdm.merge(base_destino, how="left", on="cod_ramo")
    cdm = cdm.merge(reasignados, how="left", left_on="sub_key", right_on="Maestro_Cob_Sub")
    cdm = cdm.merge(claves_agentes, how="left",on="cod_agente")

    cdm = cdm.merge(athento, how="left", left_on="nro_carpeta_digital", right_on="NumeroRadicadoAthento")
    # Creación de una llave paramétrica y combinación con el DataFrame "param"
    cdm["llave_param"] = cdm["cod_ramo"].map(str) + "_" + cdm["cod_ramo_tecnico"].map(str) +"_"+ cdm["cod_subramo_tecnico"].map(str) +"_"+ cdm["cod_ind_cob"].map(str)+"_"+cdm["Valores.Rsva-Indem_Actual"].fillna(0).astype(int).map(str)
    cdm = cdm.merge(param, how="left", left_on="llave_param", right_on = "LLAVE")
    cdm.set_index("sub_key")
    
    # Asignación de valores en la columna "agente_interm" según una condición
    cdm["agente_interm"] = cdm.apply(
                                lambda row: row["nom_agente_verif"] if pd.Series(row["nom_agente_verif"]).fillna("Nuevo")[0] !="Nuevo" else row["agente_interm"]
                                , axis=1, meta ="object")

    # Eliminación de duplicados basados en la columna "sub_key"
    cdm= cdm.drop_duplicates(subset=["sub_key"], keep="last")

    # Creación de la llave "LLAVE_ODS" y asignación de valores en columnas relacionadas con RC y reservas
    cdm["LLAVE_ODS"] = cdm[["nro_stro","cod_suc","cod_ind_cob","cod_ramo","cod_ramo_tecnico"]].apply(lambda x: "-".join(x.dropna().astype(str)),axis=1, meta = "object")
    cdm["Marca_Lesiones"] = cdm["LLAVE_ODS"].map(lesiones.set_index("llave ods")["TieneLesiones"], meta="object")
    cdm["Tipo RC"] = "Daños"
    cdm["Tipo RC"] = cdm.apply(lambda row: "Lesiones" if row["Marca_Lesiones"] == "Si" else row["Tipo RC"], axis=1 , meta = "object")
    cdm["Tipo RC"] = cdm.apply(lambda row: "Lesiones" if row["RCML"] == 1 else row["Tipo RC"], axis=1 , meta = "object")
    cdm["Tipo RC"] = cdm.apply(lambda row: "Muerte" if row["RCML"] == 2 else row["Tipo RC"], axis=1 , meta = "object")
    
    # Cambio del nombre de una columna y cálculos en columnas relacionadas con estimados e incurridos
    cdm = cdm.rename(columns={"Valores.Total Rsvas_Actual":"Valores.Total Rsva_Actual"})
    cdm["Estimado"] = cdm["Valores.Total Rsva_Actual"].fillna(0) + cdm["Valores.Total Pagos_Final"].fillna(0)
    cdm["Incurrido"] = cdm["Estimado"] - cdm["Valores.Total Recup_Final"].fillna(0)
    cdm["fec_ocurrido"] = dd.to_datetime (cdm["fec_ocurrido"],dayfirst = True) 
    
    # Asignación de valores en la columna "Estado Reserva" según condiciones
    cdm["Estado Reserva"] = cdm.apply(
        lambda row:
            "Pago total por indemnización" if pd.Series(row['Valores.Rsva-Indem_Actual']).fillna(0)[0] == 0 and pd.Series(row['Valores.Pag-Indem_Final']).fillna(0)[0] > 0
            else "Indemnización pagada parcialmente" if pd.Series(row['Valores.Rsva-Indem_Actual']).fillna(0)[0] > 0 and pd.Series(row['Valores.Pag-Indem_Final']).fillna(0)[0] > 0
            else "Reserva de indemnización abierta" if pd.Series(row['Valores.Rsva-Indem_Actual']).fillna(0)[0] > 0 and pd.Series(row['Valores.Pag-Indem_Final']).fillna(0)[0] == 0
            else  "Caso anterior a 2015" if int(pd.Series(row['fec_ocurrido'].year).fillna(2015)[0]) < 2015
            else "Cerrado sin afectación de reserva"            
            , axis=1, meta ="object")
        
        
    # Asignación de valores en la columna "Segmento" según condiciones
    cdm["Segmento"] = "Indefinido"
    cdm["Segmento"] = cdm.apply(
                            lambda row: 
                                #Condiciones para asignar valores en "Segmento"
                                    row["Reasignado a"] if not pd.isnull(row["Reasignado a"])
                                else "Secretaría General Generales" if row["Caso_Juridico"] =="JURIDICO" and row["Base"] == "Generales"
                                else "Secretaría General Vida" if row["Caso_Juridico"] =="JURIDICO"
                                else "Soat" if row["cod_ramo"] == "11" 
                                else "Coaceptado Autos" if row["cod_ramo"] in ["10","12"] and row["cod_operacion"]=="2" 
                                else "Coaceptado Vida" if  row["Base"] =="Vida" and row["cod_operacion"]=="2"
                                else "Coaceptado Otros" if row["cod_operacion"] =="2"
                                else "Asistencia" if re.match('.*ASISTENCIA.*', str(row["amparo"])) and (not re.match('.*JURIDICA.*', str(row["amparo"]))) and (not re.match('.*EXEQUIAL.*', str(row["amparo"])))
                                else "Secretaría General Generales" if row["nom_ramo"] in ["CAUCION JUDICIAL", "CUMPLIMIENTO"]
                                else "RCSP" if row["nom_ramo"] == "RESPONSABILIDAD CIVIL" and row["txt_poliza"] in["DIRECTORES Y ADMINISTRADORES SERVIDORES PUBLICOS", "SEGURO DE R.C. PARA DIRECTORES Y ADMINISTRADORES", "DIRECTORES Y ADMINISTRADORES TEXTO COLPATRIA" ]  
                                else "Graves" if row["Estimado"] > row["monto"]
                                else "RC Transportadores" if row["nom_ramo"] =="RESPONSABILIDAD CIVIL" and  re.match('.*TRANS.*', row["amparo"])
                                else "Educativo" if row["cod_ramo"] in ["79","24"]
                                else "Vida AYM" if (row["Marca_Lesiones"] == "Si" or row["Tipo RC"] in ["Lesiones","Muerte"]) and row["cod_suc"] in ["26","29"]
                                else "Vida Otros" if  row["Marca_Lesiones"] == "Si" or row["Tipo RC"] in ["Lesiones","Muerte"]
                                else "Autos-" + pd.Series(row["Amparo Agrup"]).fillna("AEV")[0] if row["cod_ramo"] in ["10","12"]
                                else "APE" if re.match('.*PERSONALES ESCOLAR.*', str(row["nom_ramo"]))
                                else "Previsionales y RV" if row["cod_ramo"] in ["55","75", "77"]
                                else "Vida AYM" if (row["Base Destino"] =="Vida" or row["Base"] =="Vida" ) and re.match('.*BANCO.*', str(row["nom_aseg"])) and not re.match('.*EDIFICIO.*', str(row["nom_aseg"]))
                                else "Complejos" if  row["cod_ramo"] in ["16","19","26","27","28","37"]
                                else "Línea Rápida" if re.match(".*DESEMPLEO.*",str(row["nom_ramo"])) or row["cod_ramo"] =="88"
                                else "Vida AYM" if row["cod_suc"] in ["26","29"] and (row["Base"]=="Vida" or row["Base Destino"] =="Vida")
                                else "Generales AYM" if row["cod_suc"] in ["26","29"]
                                else "Generales AYM" if not re.match(".*PYME-*",str(row["txt_poliza"])) and re.match("-*BANCO-*", str(row["nom_aseg"])) and not re.match(".*EDIFICIO.*",str(row["nom_aseg"]))
                                else "Vida Otros" if row["Base Destino"] =="Vida" or row["Base"] =="Vida"
                                else "Línea Rápida" if re.match( ".*ZONA COMUN.*",str(row["txt_poliza"])) or re.match(".*COPROPIEDADES.*", str(row["txt_poliza"])) or re.match(".*HOGAR.*", str(row["txt_poliza"])) or re.match(".*PYME.*", str(row["txt_poliza"])) 
                                else "Complejos"                                                   
                                , axis=1, meta ="object")  
        
        # Impresión del tipo de datos en la columna "RCML"
    print(cdm["RCML"].dtype)

                                
    """

    if os.path.exists('D:/Fuentes/Detallado Historico/cdmtotal.parquet'): 
    os.remove('D:/Fuentes/Detallado Historico/cdmtotal.parquet')
    """
    
    
    ### FORMATO UNICO DE SEGUIMIENTO A RESERVAS    
    cdm = cdm.merge(multi, how="left", left_on = "sub_key", right_on ="sub_key(mr)")
    cdm["fecha_alerta"] = fec_alerta
    cdm["fecha_alerta"] =  dd.to_datetime(cdm["fecha_alerta"], dayfirst=True)
    cdm["fecha_parametrica"] = fec_param
    cdm["fecha_parametrica"] = dd.to_datetime(cdm["fecha_parametrica"], dayfirst=True)
    cdm = cdm.assign(Alerta_param=((cdm['Ultima_modif'] < cdm['fecha_parametrica']) & (cdm["Es_Param"]=="Parametrica")).map(lambda x: 'Si' if x else 'No'))
    cdm = cdm.assign(Alerta_anual=((cdm['Ultima_modif'] < cdm['fecha_alerta']) & (cdm["Valores.Total Rsva_Actual"].fillna(0)>0)).map(lambda x: 'Si' if x else 'No'))
    
    
    # Guardar el DataFrame procesado en un archivo Parquet
    cdm.to_parquet(path_entrada + r"\3-PQT\CDM Total\cdmtotal_"+str(split)+".parquet",compression="snappy",overwrite=True )



    fin = datetime.strptime(time.strftime("%Y%m%d %I.%M.%S %p"),"%Y%m%d %I.%M.%S %p")
    tiempo_proceso = fin -inicio
    print("fin proceso"+ str(split)+": " + time.strftime("%c"))
    print("Tiempo total de procesamiento "+ str(split)+":\n" + str(tiempo_proceso))

    
    # Limpiar memoria y variables
    del cdm
    del cierre
    gc.collect()
    
    # Incrementar el año para el siguiente ciclo
    split +=1
 
    
 
    
 
    
finbuc = datetime.strptime(time.strftime("%Y%m%d %I.%M.%S %p"),"%Y%m%d %I.%M.%S %p")
tiempo_proceso_buc = finbuc -iniciobuc

print("fin proceso: " + time.strftime("%c"))
print("Tiempo total de procesamiento"+str(split)+":\n" + str(tiempo_proceso_buc))
engine = pyttsx3.init()
engine.say('El proceso a finalizado ')
engine.runAndWait()