# -*- coding: utf-8 -*-
"""
Created on Mon Nov 27 10:40:49 2023

@author: jcgarciam
"""

import win32com.client as win32
import pandas as pd
from datetime import datetime
from tkinter import ttk, Tk
import os 
import pyttsx3


now = datetime.now()

path_int1 = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\21-CDM\DATOS'

#%%

directorio = pd.read_excel(path_int1 + '/Formatos/Directorio.xlsx')


#%%
def Enviar_Correos(cierre):
    for Enviar,Nombre,enviara,copiara in zip(directorio["Enviar"].tolist(),directorio["Nombre"].tolist(),directorio["E-Mail"].tolist(),directorio["Copiar a:"].tolist()):
        
        if Enviar == "NO":
            print('No se enviará el archivo Siniestralidad por amparos ' + Nombre)
            continue
        elif (("Siniestralidad por amparos " + Nombre + ".csv") in os.listdir(path_int1 + r'\4-Salidas\3-Masivos\1-Segmentados_Intermediarios_y_Corredores')) == False:
            print('No se encuentra el archivo:')
            print("Siniestralidad por amparos " + Nombre + ".csv \n")
            continue
            
        print('Enviando correo a ', str(Nombre))
        asunto = "Siniestralidad por amparos " + Nombre + " Cierre " + cierre
        
        texto = "Adjuntamos archivo con la informacion correspondiente a los siniestros ocurridos y reportados al cierre contable del mes de " + cierre + "."
        body = "Buen día,\n" + \
                    "\n" + \
                    '\n' + \
                    texto + \
                    "\n" + \
                    "\n" + \
                    "Cordialmente,\n" + \
                    "\n" + \
                    "Business Intelligence GS\n" + \
                    "AXA COLPATRIA Seguros S.A.\n" 
                    
        adjunto = path_int1 + r"\4-Salidas\3-Masivos\1-Segmentados_Intermediarios_y_Corredores\Siniestralidad por amparos " + Nombre + ".csv"
        
        
        outlook_app = win32.Dispatch('Outlook.Application')

        mail_item = outlook_app.CreateItem(0)
        
        try:
            mail_item.Attachments.Add(adjunto)
        except FileNotFoundError:
            print("Archivo no encontrado: ", Nombre)
            continue

        mail_item.subject = asunto
        mail_item.Body = body

        mail_item.To = enviara
        mail_item.CC = copiara
        mail_item.SentOnBehalfOfName = 'BusinessIntelligence@seguros.axacolpatria.co' 

        mail_item.Send()
        
        print('Correo enviado a ', str(Nombre), '\n')
        
        i = directorio[directorio['Nombre'] == Nombre].index.tolist()[0]
        directorio["Enviar"][i] = 'NO'
        directorio.to_excel(path_int1 + '/Formatos/Directorio.xlsx', index=False) 
                    
    directorio["Enviar"] = "SI"
    directorio.to_excel(path_int1 + '/Formatos/Directorio.xlsx', index=False)  # Guardar el cambio en el archivo

#%%

window = Tk()
window.geometry("400x120")
window.title("Envio mensual de correos a intermediarios")
frame = ttk.Frame(window)
frame.grid(column=0, row=1,sticky="nsew")
label_space1 = ttk.Label(frame,text="")
label_space1.grid(column=0 ,row=0,columnspan=1)
label_advert = ttk.Label(frame,text="Ingrese de la fecha de cierre, ejemplo: Julio 2023")
label_advert.grid(column=2 ,row=1,columnspan=2)
label_space = ttk.Label(frame,text="")
label_space.grid(column=1 ,row=2,columnspan=1)
label_cierre = ttk.Label(frame, text="Cierre")
label_cierre.grid(column=2, row=3,)
entry_cierre = ttk.Entry(frame)
entry_cierre.grid(column=3, row=3)

def obtener_valor():
    cierre = entry_cierre.get().title()          
    Enviar_Correos(cierre = cierre)
    window.destroy()

button = ttk.Button(frame, text = "Enviar", command = obtener_valor)
button.grid(column=3, row=6, columnspan=3)
window.mainloop()

print('Proceso finalizado')
print('El tiempo fue de: ', datetime.now() - now)
engine = pyttsx3.init()
engine.say('El proceso a finalizado ')
engine.runAndWait() 