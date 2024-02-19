# -*- coding: utf-8 -*-
"""
Created on Mon Dec  4 08:54:55 2023

@author: jcgarciam
"""

import dask.dataframe as dd
from datetime import datetime
import pandas as pd

# Directorio de origen de los datos
archivo = "202311"

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
                             encoding = "latin1",sep=";",lineterminator=("\n"),quotechar='"',na_values=["","NA"],dayfirst=(True),low_memory=(False),decimal=",",dtype = "object"
                             )
print('Archivo ',archivo, ' leído')

#%%
suma_rsva = consolidado['rsva'].astype(str).str.replace(',','.').astype(float).sum()
suma_pag = consolidado['pag'].astype(str).str.replace(',','.').astype(float).sum()
suma_recup = consolidado['recup'].astype(str).str.replace(',','.').astype(float).sum()

print("Suma de reserva: $", '{:,.0f}'.format(suma_rsva))
print("Suma de pagos: $", '{:,.0f}'.format(suma_pag))
print("Suma de recuperaciones: $", '{:,.0f}'.format(suma_recup))

#%%
origen = r"\\DC1PVFNAS1\Autos\BusinessIntelligence\21-CDM\DATOS"

tablarsvasInfo=pd.read_parquet(origen +r"/3-PQT\Tabla_Reservas\tabla_rsvas.parquet")
a = tablarsvasInfo['Valores.Rsva-Gtos_Actual'].sum() + tablarsvasInfo['Valores.Rsva-Hono_Actual'].sum() + tablarsvasInfo['Valores.Rsva-Indem_Actual'].sum() + tablarsvasInfo['Valores.Rsva-Recob_Actual'].sum() + tablarsvasInfo['Valores.Rsva-Salv_Actual'].sum()
print("El valor total de Reservas es: $",'{:,.0f}'.format(a))

#%%

cdmInfo=pd.read_parquet(origen + '/archivo_total_stros.parquet')
a = cdmInfo['Valores.Pag-Gtos_Actual'].sum() + cdmInfo['Valores.Pag-Hono_Actual'].sum() + cdmInfo['Valores.Pag-Indem_Actual'].sum() + cdmInfo['Valores.Pag-Recob_Actual'].sum() + cdmInfo['Valores.Pag-Salv_Actual'].sum()
print("La suma total de Pagos es: $", '{:,.0f}'.format(a))
a = cdmInfo['Valores.Rsva-Gtos_Actual'].sum()+cdmInfo['Valores.Rsva-Hono_Actual'].sum()+cdmInfo['Valores.Rsva-Indem_Actual'].sum()+cdmInfo['Valores.Rsva-Recob_Actual'].sum()+cdmInfo['Valores.Rsva-Salv_Actual'].sum()
print("La suma total de Reservas es: $", '{:,.0f}'.format(a))
a = cdmInfo['Valores.Recup-Recob_Actual'].sum()+cdmInfo['Valores.Recup-Salv_Actual'].sum()
print("La suma total de Recuperaciones es: $", '{:,.0f}'.format(a))

#%%
path_entrada = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\21-CDM\DATOS'


start = 2015
dic = {}

# Procesar los datos por año y guardar en archivos parquet
while start < 2024:
    print('\n')
    print('Guardando archivo: Mixxed_', start)
    cierre = dd.read_parquet(path_entrada + r"\Por año siniestro\Mixxed_" + str(start) + ".parquet")
    dic[start] = cierre.compute()
    print('Archivo Mixxed_', start, ' guardado\n')
    start += 1
    
#%%
proceso_8 = pd.concat(dic).reset_index(drop = True)

a = proceso_8['Valores.Pag-Gtos_Actual'].sum() + proceso_8['Valores.Pag-Hono_Actual'].sum() + proceso_8['Valores.Pag-Indem_Actual'].sum() + proceso_8['Valores.Pag-Recob_Actual'].sum() + proceso_8['Valores.Pag-Salv_Actual'].sum()
print("La suma total de Pagos es: $", '{:,.0f}'.format(a))
a = proceso_8['Valores.Rsva-Gtos_Actual'].sum()+proceso_8['Valores.Rsva-Hono_Actual'].sum()+proceso_8['Valores.Rsva-Indem_Actual'].sum()+proceso_8['Valores.Rsva-Recob_Actual'].sum()+proceso_8['Valores.Rsva-Salv_Actual'].sum()
print("La suma total de Reservas es: $", '{:,.0f}'.format(a))
a = proceso_8['Valores.Recup-Recob_Actual'].sum()+proceso_8['Valores.Recup-Salv_Actual'].sum()
print("La suma total de Recuperaciones es: $", '{:,.0f}'.format(a))

#%%

path = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\21-CDM\DATOS'

# Leer el archivo CSV que contiene los campos
campos= dd.read_csv(path + r"\Formatos\Campos CDM.csv",sep=";",encoding="latin1")
campos = campos["Campo"]

# Definir los años de inicio y fin para el bucle
split = 2015
end= 2023

dic = {}
# Bucle para procesar cada año
while split <= end: 
    # Obtener la hora de inicio de la iteración actual
    print(split)   
    cdmfin= dd.read_parquet(path + r"\3-PQT\CDM Total\cdmtotal_"+str(split)+".parquet")
    dic[split] = cdmfin.compute()
    split += 1

proceso_9 = pd.concat(dic).reset_index(drop = True)
a = proceso_9['Valores.Pag-Gtos_Actual'].sum() + proceso_9['Valores.Pag-Hono_Actual'].sum() + proceso_9['Valores.Pag-Indem_Actual'].sum() + proceso_9['Valores.Pag-Recob_Actual'].sum() + proceso_9['Valores.Pag-Salv_Actual'].sum()
print("La suma total de Pagos es: $", '{:,.0f}'.format(a))
a = proceso_9['Valores.Rsva-Gtos_Actual'].sum()+proceso_9['Valores.Rsva-Hono_Actual'].sum()+proceso_9['Valores.Rsva-Indem_Actual'].sum()+proceso_9['Valores.Rsva-Recob_Actual'].sum()+proceso_9['Valores.Rsva-Salv_Actual'].sum()
print("La suma total de Reservas es: $", '{:,.0f}'.format(a))
a = proceso_9['Valores.Recup-Recob_Actual'].sum()+proceso_9['Valores.Recup-Salv_Actual'].sum()
print("La suma total de Recuperaciones es: $", '{:,.0f}'.format(a))
