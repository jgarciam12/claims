# -*- coding: utf-8 -*-
"""
Created on Thu Feb  1 09:07:16 2024

@author: jcgarciam
"""

import pandas as pd

path = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\21-CDM\DATOS\2-CSV_Anteriores'

cierre = pd.read_csv(path + '/202401.csv', 
                             encoding = "latin1",sep=";",lineterminator=("\n"),
                             quotechar='"',na_values=["","NA"],dayfirst=(True),
                             low_memory=(False),decimal=",",dtype = "object"
                             )

cierre = cierre[cierre['Mes Contable'].isnull() == False]

#%%
cierre['Cod_Base'] = ''
cierre.loc[cierre['Base'] == 'Generales','Cod_Base'] = '1'
cierre.loc[cierre['Base'] == 'Vida','Cod_Base'] = '0'
cierre["sub_key"] = cierre["cod_ramo"].map(str) + "_" + cierre["cod_ramo_tecnico"].map(str) + "_" + cierre["cod_suc"].map(str) + "_" +cierre["nro_stro"].map(str) +"_" + cierre["cod_ind_cob"].map(str) + "_" + cierre["cod_tercero"].map(str) + "_" + cierre["cod_item"].map(str) +"-"+ cierre["Cod_Base"].map(str) # Se crea llave de subsiniestro
cierre['rsva'] = cierre['rsva'].astype(str).str.replace(',','.').astype(float)
cierre = cierre[cierre['error'] == 'OK']
cierre = cierre[cierre['cod_ramo'] != '11']
cierre = cierre[cierre['Cod_Base'] == '1']

#%%

print('$ {:,.0f}'.format(cierre['rsva'].astype(str).str.replace(',','.').astype(float).sum()))

#%%

path2 = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\21-CDM\DATOS\4-Salidas\2-Consolidados'

solo_gen_vida = pd.read_csv(path2 + '/1-Subsiniestros-Claims_Data_Manager-Autos_Gles_y_Vida.csv',
                            sep = ';', encoding = 'ansi')

solo_asis = pd.read_csv(path2 + '/2-Subsiniestros-Claims_Data_Manager-Asistencias.csv',
                            sep = ';', encoding = 'ansi')

#solo_soat = pd.read_csv(path2 + '/3-Subsiniestros-Claims_Data_Manager-Soat.csv',
#                            sep = ';', encoding = 'ansi')

#%%

a = pd.concat([solo_gen_vida,solo_asis])
a = a[a['Base'] == 'Generales']
b = a['Valores.Total Rsva_Actual'].astype(str).str.replace(',','.').astype(float).sum()

print('$ {:,.0f}'.format(b))

#%%
sin_errores = cierre.loc[cierre['error'] == 'OK', 'sub_key']
cruce = a[a["sub_key"].isin(cierre['sub_key']) == True]
print('$ {:,.0f}'.format(cruce['Valores.Total Rsva_Actual'].astype(str).str.replace(',','.').astype(float).sum()))
print('$ {:,.0f}'.format(cierre['rsva'].astype(str).str.replace(',','.').astype(float).sum()))

#%%
resumen_cierre = cierre.copy()
resumen_cierre['rsva'] = resumen_cierre['rsva'].astype(str).str.replace(',','.').astype(float)
resumen_cierre = resumen_cierre.groupby('cod_ramo_tecnico', as_index = False)['rsva'].sum()
totale_cierre = resumen_cierre['rsva'].sum().round()
total_cierre = {'rsva':totale_cierre}
resumen_cierre['cod_ramo_tecnico'] = resumen_cierre['cod_ramo_tecnico'].astype(int)
resumen_cierre = resumen_cierre.append(total_cierre, ignore_index=True)
resumen_cierre['cod_ramo_tecnico'] = resumen_cierre['cod_ramo_tecnico'].fillna('Total')

resumen_cruce = cruce.copy()
resumen_cruce['Valores.Total Rsva_Actual'] = resumen_cruce['Valores.Total Rsva_Actual'].astype(str).str.replace(',','.').astype(float)
resumen_cruce = resumen_cruce.groupby('cod_ramo_tecnico', as_index = False)['Valores.Total Rsva_Actual'].sum()
totale_cruce = resumen_cruce['Valores.Total Rsva_Actual'].sum().round()
total_cruce = {'Valores.Total Rsva_Actual':totale_cruce}
resumen_cruce['cod_ramo_tecnico'] = resumen_cruce['cod_ramo_tecnico'].astype(int)
resumen_cruce = resumen_cruce.append(total_cruce, ignore_index=True)
resumen_cruce['cod_ramo_tecnico'] = resumen_cruce['cod_ramo_tecnico'].fillna('Total')

#%%

c = resumen_cruce.merge(resumen_cierre, how = 'left', on = 'cod_ramo_tecnico')
c['dif'] = c['Valores.Total Rsva_Actual'] - c['rsva']
c['Valores.Total Rsva_Actual'] = '$ ' + c['Valores.Total Rsva_Actual'].apply('{:,.0f}'.format)
c['rsva'] = '$ ' + c['rsva'].apply('{:,.0f}'.format)
c['dif'] = '$ ' + c['dif'].apply('{:,.0f}'.format)
c.to_excel(r'D:\DATOS\Users\jcgarciam\Downloads/comparacion_contable_VS_claims_reserva_generales.xlsx', index = False)





