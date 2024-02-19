# -*- coding: utf-8 -*-
"""
Created on Sun Dec  3 20:02:30 2023

@author: jcgarciam
"""

import pandas as pd
import os 

path = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\21-CDM\DATOS\4-Salidas\1-Datos_por_ocurrencia'

dic = {}
for i in os.listdir(path + '/3-Generales_y_Vida'):
    print(i)
    df = pd.read_csv(path + '/3-Generales_y_Vida/' + i, sep=";", encoding = 'ANSI')
    dic[i] = df
    df['Origen'] = '3-Generales_y_Vida'
    
for i in os.listdir(path + '/2-Asistencias'):
    print(i)
    df = pd.read_csv(path + '/2-Asistencias/' + i, sep=";", encoding = 'ANSI')
    dic[i] = df
    df['Origen'] = '2-Asistencias'

for i in os.listdir(path + '/1-Autos_Sin_Asistencias'):
    print(i)
    df = pd.read_csv(path + '/1-Autos_Sin_Asistencias/' + i, sep=";", encoding = 'ANSI')
    dic[i] = df
    df['Origen'] = '1-Autos_Sin_Asistencias'

for i in os.listdir(path + '/4-Soat'):
    print(i)
    df = pd.read_csv(path + '/4-Soat/' + i, sep=";", encoding = 'ANSI')
    dic[i] = df
    df['Origen'] = '4-Soat'

    
#%%

df = pd.concat(dic).reset_index(drop = True)
df = df.drop_duplicates('sub_key')
    
#%%

a = df['Valores.Pag-Gtos_Actual'].astype(str).str.replace(',','.').astype(float).sum() + df['Valores.Pag-Hono_Actual'].astype(str).str.replace(',','.').astype(float).sum() + df['Valores.Pag-Indem_Actual'].astype(str).str.replace(',','.').astype(float).sum() + df['Valores.Pag-Recob_Actual'].astype(str).str.replace(',','.').astype(float).sum() + df['Valores.Pag-Salv_Actual'].astype(str).str.replace(',','.').astype(float).sum()

print('Total\n')

print("La suma total de Pagos es: $", '{:,.0f}'.format(a))

a = df['Valores.Recup-Recob_Actual'].astype(str).str.replace(',','.').astype(float).sum()+df['Valores.Recup-Salv_Actual'].astype(str).str.replace(',','.').astype(float).sum()

print("La suma total de Recuperaciones es: $", '{:,.0f}'.format(a))

a = df['Valores.Rsva-Gtos_Actual'].astype(str).str.replace(',','.').astype(float).sum()+df['Valores.Rsva-Hono_Actual'].astype(str).str.replace(',','.').astype(float).sum()+df['Valores.Rsva-Indem_Actual'].astype(str).str.replace(',','.').astype(float).sum()+df['Valores.Rsva-Recob_Actual'].astype(str).str.replace(',','.').astype(float).sum()+df['Valores.Rsva-Salv_Actual'].astype(str).str.replace(',','.').astype(float).sum()

print("La suma total de Reserva es: $", '{:,.0f}'.format(a))
#%%

print('\n')
print('Generales\n')
df = df[df['Base'] == 'Generales']

a = df['Valores.Pag-Gtos_Actual'].astype(str).str.replace(',','.').astype(float).sum() + df['Valores.Pag-Hono_Actual'].astype(str).str.replace(',','.').astype(float).sum() + df['Valores.Pag-Indem_Actual'].astype(str).str.replace(',','.').astype(float).sum() + df['Valores.Pag-Recob_Actual'].astype(str).str.replace(',','.').astype(float).sum() + df['Valores.Pag-Salv_Actual'].astype(str).str.replace(',','.').astype(float).sum()

print("La suma total de Pagos es: $", '{:,.1f}'.format(a))

a = df['Valores.Recup-Recob_Actual'].astype(str).str.replace(',','.').astype(float).sum()+df['Valores.Recup-Salv_Actual'].astype(str).str.replace(',','.').astype(float).sum()

print("La suma total de Recuperaciones es: $", '{:,.1f}'.format(a))

a = df['Valores.Rsva-Gtos_Actual'].astype(str).str.replace(',','.').astype(float).sum()+df['Valores.Rsva-Hono_Actual'].astype(str).str.replace(',','.').astype(float).sum()+df['Valores.Rsva-Indem_Actual'].astype(str).str.replace(',','.').astype(float).sum()+df['Valores.Rsva-Recob_Actual'].astype(str).str.replace(',','.').astype(float).sum()+df['Valores.Rsva-Salv_Actual'].astype(str).str.replace(',','.').astype(float).sum()

print("La suma total de Reserva es: $", '{:,.1f}'.format(a))

#%%




    
