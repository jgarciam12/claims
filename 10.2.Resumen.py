# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import pandas as pd

subsiniestros_2 = pd.read_csv(r'\\DC1PVFNAS1\Autos\BusinessIntelligence\21-CDM\DATOS\4-Salidas\2-Consolidados/2-Subsiniestros-Claims_Data_Manager-Asistencias.csv', sep = ';', encoding = 'ANSI')

subsiniestros_1 = pd.read_csv(r'\\DC1PVFNAS1\Autos\BusinessIntelligence\21-CDM\DATOS\4-Salidas\2-Consolidados/1-Subsiniestros-Claims_Data_Manager-Autos_Gles_y_Vida.csv', sep = ';', encoding = 'ANSI')

subsiniestros_3 = pd.read_csv(r'\\DC1PVFNAS1\Autos\BusinessIntelligence\21-CDM\DATOS\4-Salidas\2-Consolidados\3-Subsiniestros-Claims_Data_Manager-Soat.csv', sep = ';', encoding = 'ANSI')

#%%
print('Asistencias: \n')
print('Pagos: ${:,.0f}'.format(subsiniestros_2['Valores.Total Pagos_Actual'].astype(str).str.replace(',','.').astype(float).sum()))
print('Recuperaciones: ${:,.0f}'.format(subsiniestros_2['Valores.Total Recup_Actual'].astype(str).str.replace(',','.').astype(float).sum()))
print('Reserva: ${:,.0f}'.format(subsiniestros_2['Valores.Total Rsva_Actual'].astype(str).str.replace(',','.').astype(float).sum()))
print('')
print('Generales: \n')
print('Pagos: ${:,.0f}'.format(subsiniestros_1.loc[subsiniestros_1['Base'] == 'Generales','Valores.Total Pagos_Actual'].astype(str).str.replace(',','.').astype(float).sum()))
print('Recuperaciones: ${:,.0f}'.format(subsiniestros_1.loc[subsiniestros_1['Base'] == 'Generales','Valores.Total Recup_Actual'].astype(str).str.replace(',','.').astype(float).sum()))
print('Reserva: ${:,.0f}'.format(subsiniestros_1.loc[subsiniestros_1['Base'] == 'Generales','Valores.Total Rsva_Actual'].astype(str).str.replace(',','.').astype(float).sum()))
print('')
print('Generales y Asistencias: \n')
print('Pagos: ${:,.0f}'.format(subsiniestros_1.loc[subsiniestros_1['Base'] == 'Generales','Valores.Total Pagos_Actual'].astype(str).str.replace(',','.').astype(float).sum()+
      subsiniestros_2['Valores.Total Pagos_Actual'].astype(str).str.replace(',','.').astype(float).sum()))
print('Recuperaciones: ${:,.0f}'.format(subsiniestros_1.loc[subsiniestros_1['Base'] == 'Generales','Valores.Total Recup_Actual'].astype(str).str.replace(',','.').astype(float).sum()+
      subsiniestros_2['Valores.Total Recup_Actual'].astype(str).str.replace(',','.').astype(float).sum()))
print('Reserva: ${:,.0f}'.format(subsiniestros_1.loc[subsiniestros_1['Base'] == 'Generales','Valores.Total Rsva_Actual'].astype(str).str.replace(',','.').astype(float).sum()+
                                 subsiniestros_2['Valores.Total Rsva_Actual'].astype(str).str.replace(',','.').astype(float).sum()))

print('')
print('Vida: \n')
print('Pagos: ${:,.0f}'.format(subsiniestros_1.loc[subsiniestros_1['Base'] == 'Vida','Valores.Total Pagos_Actual'].astype(str).str.replace(',','.').astype(float).sum()))
print('Recuperaciones: ${:,.0f}'.format(subsiniestros_1.loc[subsiniestros_1['Base'] == 'Vida','Valores.Total Recup_Actual'].astype(str).str.replace(',','.').astype(float).sum()))
print('Reserva: ${:,.0f}'.format(subsiniestros_1.loc[subsiniestros_1['Base'] == 'Vida','Valores.Total Rsva_Actual'].astype(str).str.replace(',','.').astype(float).sum()))
print('')
print('Soat: \n')
print('Pagos: ${:,.0f}'.format(subsiniestros_3['Valores.Total Pagos_Actual'].astype(str).str.replace(',','.').astype(float).sum()))
print('Recuperaciones: ${:,.0f}'.format(subsiniestros_3['Valores.Total Recup_Actual'].astype(str).str.replace(',','.').astype(float).sum()))
print('Reserva: ${:,.0f}'.format(subsiniestros_3['Valores.Total Rsva_Actual'].astype(str).str.replace(',','.').astype(float).sum()))
print('')
print('Total: \n')
print('Pagos: ${:,.0f}'.format(subsiniestros_3['Valores.Total Pagos_Actual'].astype(str).str.replace(',','.').astype(float).sum()+
      subsiniestros_1['Valores.Total Pagos_Actual'].astype(str).str.replace(',','.').astype(float).sum()+
      subsiniestros_2['Valores.Total Pagos_Actual'].astype(str).str.replace(',','.').astype(float).sum()))
print('Recuperaciones: ${:,.0f}'.format(subsiniestros_3['Valores.Total Recup_Actual'].astype(str).str.replace(',','.').astype(float).sum()+
      subsiniestros_2['Valores.Total Recup_Actual'].astype(str).str.replace(',','.').astype(float).sum()+
      subsiniestros_1['Valores.Total Recup_Actual'].astype(str).str.replace(',','.').astype(float).sum()))
print('Reserva: ${:,.0f}'.format(subsiniestros_3['Valores.Total Rsva_Actual'].astype(str).str.replace(',','.').astype(float).sum()+
                                 subsiniestros_2['Valores.Total Rsva_Actual'].astype(str).str.replace(',','.').astype(float).sum()+
                                 subsiniestros_1['Valores.Total Rsva_Actual'].astype(str).str.replace(',','.').astype(float).sum()))