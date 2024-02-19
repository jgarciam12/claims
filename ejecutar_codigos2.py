# -*- coding: utf-8 -*-
"""
Created on Tue Jan 30 14:23:20 2024

@author: jcgarciam
"""

path = r'D:\DATOS\Users\jcgarciam\OneDrive - AXA Colpatria Seguros\Documentos\Informes\SISCO\Script'

path_funciones = path +'/Sisco_Salud_Consultas.py'
try:
    with open(path_funciones, 'r') as file: script_codigo = file.read()
    print('Va a correr el script de funciones')
    exec(script_codigo)
except FileExistsError():
    print(f'Error: El archivo {path_funciones} no fue encontrado.')