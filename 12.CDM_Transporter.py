# -*- coding: utf-8 -*-
"""
Created on Tue Mar 28 20:58:26 2023

@author: Edgaviriac
"""

import shutil
import time
import winsound
from datetime import datetime

path = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\21-CDM\DATOS\4-Salidas\2-Consolidados'

inicio = datetime.strptime(time.strftime("%Y%m%d %I.%M.%S %p"),"%Y%m%d %I.%M.%S %p")
print(inicio)

lista = ['1-Subsiniestros-Claims_Data_Manager-Autos_Gles_y_Vida.csv',
          '2-Subsiniestros-Claims_Data_Manager-Asistencias.csv',
          '3-Subsiniestros-Claims_Data_Manager-Soat.csv',
          '4-Subsiniestros-Claims_Data_Manager-Solo_Gles_y_Vida.csv',
          '5-Subsiniestros-Claims_Data_Manager-Solo_Autos.csv',
          '6-Subsiniestros-Claims_Data_Manager-Autos_con_Asistencias.csv',
          '7-Amparos-Claims_Data_Manager-Autos_Gles_y_Vida.csv',
          '8-Amparos-Claims_Data_Manager-Asistencias.csv',
          '9-Amparos-Claims_Data_Manager-Soat.csv',
          '10-Amparos-Claims_Data_Manager-Solo_Gles_y_Vida.csv',
          '11-Amparos-Claims_Data_Manager-Solo_Autos.csv',
          '12-Amparos-Claims_Data_Manager-Autos_con_Asistencias.csv',
          '13-Siniestros-Claims_Data_Manager-Autos_Gles_y_Vida.csv',
          '14-Siniestros-Claims_Data_Manager-Asistencias.csv',
          '15-Siniestros-Claims_Data_Manager-Soat.csv',
          '16-Siniestros-Claims_Data_Manager-Solo_Gles_y_Vida.csv',
          '17-Siniestros-Claims_Data_Manager-Solo_Autos.csv',
          '18-Siniestros-Claims_Data_Manager-Autos_con_Asistencias.csv',
          '19-Subsiniestros-Claims_Data_Manager-Consulta_Gles_y_Vida.csv',
          '20-Subsiniestros-Claims_Data_Manager-Consulta_Autos.csv'
          ]


for i in lista:
    print('Guardando el archivo: ',i)
    shutil.copy(path + r"/" + i,
            r"\\dc1pvfnas1\Autos\BusinessIntelligence\16-Claims_Data_Manager/" + i)
    
    print("El archivo " + i + " ha sido copiado con exito en el servidor de destino a las " + str(datetime.strptime(time.strftime("%Y%m%d %I.%M.%S %p"),"%Y%m%d %I.%M.%S %p")) + '\n' )
    
    
    
    
fin = datetime.strptime(time.strftime("%Y%m%d %I.%M.%S %p"),"%Y%m%d %I.%M.%S %p")
tiempo_proceso = fin -inicio
print("fin proceso: " + time.strftime("%c"))
print("Tiempo total de procesamiento:\n" + str(tiempo_proceso))
winsound.MessageBeep(0X00000000) 