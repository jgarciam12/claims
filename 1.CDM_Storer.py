# -*- coding: utf-8 -*-
"""
Created on Thu Feb  2 14:23:10 2023

@author: Edgaviriac
"""

print("inicio")


import pandas as pd
import pyarrow.parquet as pq
from datetime import datetime
import time
import pyttsx3


#%%
inicio = datetime.strptime(time.strftime("%Y%m%d %I.%M.%S %p"),"%Y%m%d %I.%M.%S %p")

# Define el nombre del archivo a procesar.
archivo = "202401"

#%%
# Define una función para analizar una cadena de fecha y convertirla en un objeto datetime.
def parse_date(date_str): 
    return pd.datetime.strptime(date_str, '%d/%m/%Y')


# Ruta donde se encuentran los archivos a procesar.
path_int = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\21-CDM\DATOS'
#rutadetallado = r"D:\DATOS\2-CSV_Anteriores/"

#archivos = os.listdir(rutadetallado) #Se listan los archivos contenidos en la ruta.
#primerarchivo = min(archivos) #Se identifica cual es el primer archivo en la lista por orden alfabetico para iniciar mas adelante el proceso de consolidación.

#año = primerarchivo
#año = año.replace(".csv","")
# Lee los encabezados y tipos de datos desde un archivo Excel.
rutainicial = path_int + '/2-CSV_Anteriores/' + str(archivo) #Se define ruta completa del archivo con el que se iniciará proceso de consolidación.
# = {"nro_pol":"object","nro_comprobante":"object","nro_doc":"object","imp_estimado":"object","imp_pago":"object"}
encabezados= pd.read_excel(io = path_int  + r"\Formatos\Encabezado.xlsx", sheet_name = 'Data')
columnsfechas= pd.read_excel(io = path_int  + r"\Formatos\Encabezado.xlsx",sheet_name= "ColDate")
dtypes_select = dict(zip(encabezados["nombre"],encabezados["tipo"]))
col_dates =  columnsfechas["nombre"].tolist()

print(archivo) 

# Lee el archivo CSV con el nombre construido utilizando la ruta y el nombre del archivo.
print('Leyendo el archivo ',archivo)
consolidado = pd.read_csv(path_int + '/2-CSV_Anteriores/' + archivo + '.csv',
                             encoding = "latin1",sep=";",lineterminator=("\n"),
                             quotechar='"',na_values=["","NA"],dayfirst=(True),
                             low_memory=(False),decimal=",",dtype = "object"
                             )
print('Archivo ',archivo, ' leído')
#%%
###Repara datos numericos y fechas

# Crea un DataFrame llamado "codigos_bases" con códigos base y sus nombres.
consolidado['Cod_Base'] = ''
consolidado.loc[consolidado['Base'] == 'Generales','Cod_Base'] = '1'
consolidado.loc[consolidado['Base'] == 'Vida','Cod_Base'] = '0'


# Crea nuevas columnas de llaves utilizando combinaciones de columnas existentes.
consolidado["sub_key"] = consolidado["cod_ramo"].map(str) + "_" + consolidado["cod_ramo_tecnico"].map(str) + "_" + consolidado["cod_suc"].map(str) + "_" +consolidado["nro_stro"].map(str) +"_" + consolidado["cod_ind_cob"].map(str) + "_" + consolidado["cod_tercero"].map(str) + "_" + consolidado["cod_item"].map(str) +"-"+ consolidado["Cod_Base"].map(str) # Se crea llave de subsiniestro
consolidado["amp_key"] = consolidado["cod_ramo"].map(str) + "_" + consolidado["cod_ramo_tecnico"].map(str) + "_" + consolidado["cod_suc"].map(str) + "_" +consolidado["nro_stro"].map(str) + "_" + consolidado["cod_ind_cob"].map(str)  +"-"+ consolidado["Cod_Base"].map(str)  # Se crea llave de amparo
consolidado["sin_key"] = consolidado["cod_ramo"].map(str) + "_" + consolidado["cod_ramo_tecnico"].map(str) + "_" + consolidado["cod_suc"].map(str) + "_" +consolidado["nro_stro"].map(str) +"-"+ consolidado["Cod_Base"].map(str) # Se crea llave de siniestro

# Convierte columnas de fecha a objetos datetime.
fechas = ['fec_vig_hasta','fec_vig_desde','fec_formalizado','Mes Contable','fec_emi']

for i in fechas:
    consolidado[i] = pd.to_datetime(consolidado[i],dayfirst = True)

# Realiza limpieza y conversión de columnas numéricas.
datos_numericos = ['danios_materiales','pago_danios_materiales','lesiones_personales','pago_lesiones_personales',
                   'victimas_por_muerte','pago_victimas_por_muerte','pag','recup','rsva']

for i in datos_numericos:
    consolidado[i] = consolidado[i].str.replace(",",".")
    consolidado[i] = pd.to_numeric(consolidado[i])

# Crea una columna "RCML" y asigna valores basados en condiciones.
consolidado['RCML'] = 0 
consolidado.loc[(consolidado['victimas_por_muerte'] + consolidado['pago_victimas_por_muerte']) > 0, 'RCML'] = 2 
consolidado.loc[(consolidado['lesiones_personales'] + consolidado['pago_lesiones_personales']) > 0 & (consolidado['RCML'] == 0), 'RCML'] = 1

# Crea una columna "Detalle_SP" concatenando valores de diferentes columnas.
consolidado["Detalle_SP"] = consolidado['nro_aut_tec'].astype(str).str.replace(".0","") + "/" + consolidado['fec_emi'].dt.day.astype(str).str.replace(".0","") + "." + consolidado['fec_emi'].dt.month.astype(str).str.replace(".0","") + "." + consolidado['fec_emi'].dt.year.astype(str).str.replace(".0","") + "/" + consolidado['imp_cpto'].astype(str).str.replace(".0","") + "/" + "[" + consolidado['txt_cheque_a_nom'] +"]"


#%%

# Crea un DataFrame "new" con valores únicos de la columna "sub_key".
new = pd.DataFrame(consolidado["sub_key"].unique(), columns=["sub_key"])
new["vigente"] ="si"

# Lee el archivo "Key_sub_stock.parquet" y almacena su contenido en "stock".
stock = pq.read_pandas(path_int + r"/Key_sub_stock.parquet").to_pandas()
# Guarda el DataFrame "stock" en un nuevo archivo parquet con un nombre que incluye la fecha actual.
stock.to_parquet(path_int + r"/Key_sub_stock_act-"+archivo+"_" + format(datetime.now().strftime("%Y%m%d_%H%M%S"))+".parquet",compression= "snappy")
# Agrega una columna "en_stock" al DataFrame "stock".
stock["en_stock"] = "si"
stock = stock[["sub_key","en_stock"]]
# Realiza una serie de operaciones de combinación y filtrado de DataFrames.
verify = pd.merge(stock,new,how = "inner", on = "sub_key")
verify = verify.drop_duplicates(subset = "sub_key", keep = "last")
add = pd.merge(stock, new, how = "right", on = "sub_key" )
add = add.drop_duplicates(subset = "sub_key", keep = "last")
add = add[(add["vigente"] == "si") & (add["en_stock"].isnull())]
add["agregar"] = "si"
# Concatena los DataFrames y actualiza la columna "en_stock".
stock= pd.concat([stock,new],axis=0)
stock["en_stock"] = "si"
stock = stock.drop_duplicates(subset="sub_key", keep="last")
# Guarda el DataFrame "stock" en el archivo "Key_sub_stock.parquet".
stock.to_parquet(path_int + r"/Key_sub_stock.parquet",compression= "snappy")
# Guarda el DataFrame "verify" en el archivo "Key_sub_verify.parquet".
verify.to_parquet(path_int + r"/Key_sub_verify.parquet",compression= "snappy")


print(time.strftime("%c") +" inicia el proceso de formateo numerico")

#%%

# Selecciona los campos relacionados con pagos, recuperaciones, reservas, generales y lesiones.
campospagos = encabezados.loc[encabezados["tablapagos"] == "incluir"]
campospagos = campospagos["nombre"].tolist()
camposrecup = encabezados.loc[encabezados["tablarecup"] == "incluir"]
camposrecup = camposrecup["nombre"].tolist()
camposrsvas = encabezados.loc[encabezados["tablarsvas"] == "incluir"]
camposrsvas = camposrsvas["nombre"].tolist()
camposgen = encabezados.loc[encabezados["datosgenerales"] == "incluir"]
camposgen = camposgen["nombre"].tolist()
camposlesiones = encabezados.loc[encabezados["tablalesiones"] == "incluir"]
camposlesiones = camposlesiones["nombre"].tolist()
#%%
filtro =  consolidado.loc[consolidado["concepto"]=="Pag"]["fuente"].unique().astype(str).tolist()

# Filtra los datos en el DataFrame "consolidado" y guarda los resultados en archivos parquet según los detalles de movimiento.
for detallemov in filtro:
    nombrearchivo = path_int + r"/3-PQT/Pag/" + detallemov + "_" + archivo + ".parquet"
    valores = consolidado.loc[consolidado["fuente"]== detallemov]
    valores = valores[campospagos]
    print(time.strftime("%c") +" inicia el poceso de escritura de archivo "+ detallemov+" en parquet")
    valores.to_parquet(nombrearchivo, compression = "snappy")

filtro =  consolidado.loc[consolidado["concepto"]=="Recup"]["fuente"].unique().astype(str).tolist()
for detallemov in filtro:
    nombrearchivo = path_int + r"/3-PQT/Recup/"+detallemov+"_"+archivo+".parquet"
    valores = consolidado.loc[consolidado["fuente"]== detallemov]
    valores = valores[camposrecup] 
    print(time.strftime("%c") +" inicia el poceso de escritura de archivo "+ detallemov+" en parquet")
    valores.to_parquet(nombrearchivo, compression = "snappy")

filtro =  consolidado.loc[consolidado["concepto"]=="Rsva"]["fuente"].unique().astype(str).tolist()
for detallemov in filtro:
    nombrearchivo = path_int + r"/3-PQT/Rsvas/"+detallemov+"_"+archivo+".parquet"
    valores = consolidado.loc[consolidado["fuente"]== detallemov] 
    valores = valores[camposrsvas]  
    print(time.strftime("%c") +" inicia el poceso de escritura de archivo "+ detallemov+" en parquet")
    valores.to_parquet(nombrearchivo, compression = "snappy")

# Filtra y procesa los datos generales y los guarda en un archivo parquet.
nombrearchivodg = path_int + r"/3-PQT/Datos_Generales/Datos_Generales_"+archivo+".parquet"
datgen = consolidado.drop_duplicates(subset="sub_key", keep="last")
datgen = datgen.merge(add,how="right", on ="sub_key")
datgen = datgen[datgen["agregar"]=="si"]
datgen = datgen[camposgen]
#%%
print(time.strftime("%c") +" inicia el poceso de escritura de archivo datos generales en parquet")
datgen.to_parquet(nombrearchivodg, compression = "snappy")

#%%

# Realiza una serie de operaciones de agrupación y cálculo y guarda los resultados en un archivo parquet.
RC_Muerte_Lesiones =  consolidado.loc[consolidado["RCML"]>0]
nombrearchivo = path_int + r"/3-PQT/RCML/"+"RCML"+"_"+archivo+".parquet"
RC_Muerte_Lesiones = RC_Muerte_Lesiones.groupby("sub_key")["RCML","rsva"].max()
RC_Muerte_Lesiones.to_parquet(nombrearchivo, compression = "snappy")

# Filtra y procesa datos para casos jurídicos y guarda los resultados en un archivo parquet.
juridico = consolidado[["sub_key","Mes Contable","txt_est_stro","fec_emi"]].copy()
juridico = juridico[juridico["txt_est_stro"].fillna("Ninguno").str.contains("JURIDIC")]
juridico = juridico.sort_values(by=["Mes Contable","fec_emi"], ascending =[True,True])
juridico = juridico.groupby("sub_key")["txt_est_stro"].last().reset_index()
juridico.to_parquet(path_int + r"/3-PQT/Juridicos/"+"Casos_juridicos"+"_"+archivo+".parquet")

#print(time.strftime("%c") +" inicia el poceso de lectura de archivo parquet")
#auda = pq.read_pandas(nombrearchivo).to_pandas()
#auda.to_csv(r"D:\elparq.csv",sep=";",index= None, decimal=",",encoding = "Latin-1")
fin = datetime.strptime(time.strftime("%Y%m%d %I.%M.%S %p"),"%Y%m%d %I.%M.%S %p")
tiempo_proceso = fin -inicio
suma_rsva = consolidado['rsva'].sum()
suma_pag = consolidado['pag'].sum()
suma_recup = consolidado['recup'].sum()
print('\nResumen\n:')
print("Suma de reserva: $", '{:,.0f}'.format(suma_rsva))
print("Suma de pagos: $", '{:,.0f}'.format(suma_pag))
print("Suma de recuperaciones: $", '{:,.0f}'.format(suma_recup))

print("fin proceso: "+str(archivo)+" " + time.strftime("%c"))
print("Tiempo total de procesamiento:\n" + str(tiempo_proceso))
engine = pyttsx3.init()
engine.say('El proceso a finalizado ')
engine.runAndWait()