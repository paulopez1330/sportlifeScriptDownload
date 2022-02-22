'''
Capturara la data de Sportlife, desde DTG
'''
import json
import requests
import creeds
import pandas as pd
from datetime import date
import datetime
import sys
import utils
import os
##############################################################################################################

#### ---- Obtenemos el path actual ---- ####
path_current                            =   os.path.dirname(os.path.abspath(__file__))
list_path_current_separate_by_slash     =   path_current.split("/")

#### ---- Nos quedamos con el path, menos la ultima carpeta, la que contiene esta archivo ---- ####
list_path_current_folder                =   path_current.split(str(list_path_current_separate_by_slash[-2]))

#### ---- Armamos el path ---- ####
path_project_callegari                  =   list_path_current_folder[0]
path_project_callegari                  =   path_project_callegari + "Panel_Interno"
print(path_project_callegari)
sys.path.append(os.path.abspath(path_project_callegari))

import insertar
Proceso =   insertar.Estado_Proceso()
##############################################################################################################

today = date.today()
total_usuarios = []
class capture_data():
    #### ---- Necesarias para hacer la peticion ---- ####
    url             =   "https://sportlifesa.grupodtg.com/api/karrot/getUserInfo"
    url_listablanca =   "https://sportlifesa.grupodtg.com/api/karrot/getStatus/?lastrow=0&clubid="
    token           =   creeds.token

    #### ---- En estas variables guardaremos las distintas informaciones de los usuarios ---- ####
    usuario_info        =   []
    usuario_contrato    =   []
    usuario_asistencia  =   []

    #### ---- Archivo con los usuarios de entrada ---- ####
    file_de_usuarios    =   "s3://karrott-sporlife/raw/estado_clientes.csv"

    #### ----- Ruta de los archivos de salida en S3 ----- ####
    salida_diaria       =   "s3://karrott-sporlife/raw/diario/gyms/"

    #### ---- Fecha de la captura ---- ####
    fecha_hoy           =   today.strftime("%Y-%m-%d")

    def recorre_usuarios_csv(self, **kwargs):
        #### ---- Leemos el archivos y transformamos a lista ----- ####
        usuarios    =   pd.read_csv(self.file_de_usuarios, delimiter=';', error_bad_lines=False)

        
        #### ---- Definimos los tipos de usuarios a retornar ---- ####
        if kwargs["tipo_usuario"]=="todos":         usuarios_filtrados  =   usuarios
        elif kwargs["tipo_usuario"] == "activos":   usuarios_filtrados  =   usuarios[usuarios['finalizado']=='N']
        elif kwargs["tipo_usuario"] == "inactivos": usuarios_filtrados  =   usuarios[usuarios['finalizado']=='Y']
        else:
            print("Tipo de usuario a leer no reconocido: todos | activos | inactivos")
            return

        #### ---- Convertimos a lista y recorremos obteniendo la informacion ---- ####
        i=0
        lista_usuarios  =   usuarios_filtrados["id_cliente"].to_list()
        for usuario in lista_usuarios:
            try:
                self.get_data(rut=str(usuario))
            except Exception as e:
                print(e)
            i+=1
            print (i)



        #### ---- Formateamos los nombres ---- ####
        today               =   date.today()
        today               =   today.strftime("%m_%d_%Y")
        info_nombre         =   '{}info_{}_{}.csv'.format(self.salida_diaria, kwargs["tipo_usuario"], today)
        asistencia_nombre   =   '{}asistencia_{}_{}.csv'.format(self.salida_diaria, kwargs["tipo_usuario"], today)
        contrato_nombre     =   '{}contrato_{}_{}.csv'.format(self.salida_diaria, kwargs["tipo_usuario"], today)


        #### ---- Guardamos los archivos como csv locales ---- ####
        #self.save_csv(lista=self.usuario_info,          nombre=info_nombre)
        #self.save_csv(lista=self.usuario_asistencia,    nombre=asistencia_nombre)
        #self.save_csv(lista=self.usuario_contrato,      nombre=contrato_nombre)

        #### ---- Guardamos en S3 como csv.gz ---- ####
        utils.save_csv_gz(self.listdict_to_dataframe(lista=self.usuario_info), info_nombre)
        utils.save_csv_gz(self.listdict_to_dataframe(lista=self.usuario_asistencia), asistencia_nombre)
        utils.save_csv_gz(self.listdict_to_dataframe(lista=self.usuario_contrato), contrato_nombre)

    
    def recorremos_listablanca(self, **kwargs):
        '''
        Recorremos la lista blanca
        --------------------------
        Recorremos la lista blanca de usuarios para obtener su informacion
        '''

        #### ---- Definimos el header ---- ####
        headers     =   {"Authorization": self.token}

        #### ---- Creamos el header ---- ####
        url         =   self.url_listablanca + str(kwargs["idgym"])
        solicitud   =   requests.get(url, headers=headers)
        response    =   solicitud.json()

        print(" Capturando")
        for cliente in response["data"]:
            #### ---- Si es una solicitud valida ---- ####
            if response["status"] == 200:
                self.get_data(rut=str(cliente["Rut"]))

        #### ---- Formateamos los nombres ---- ####
        today               =   date.today()
        today               =   today.strftime("%m_%d_%Y")
        info_nombre         =   '{}info_{}_{}.csv'.format(self.salida_diaria,  today, kwargs["idgym"])
        asistencia_nombre   =   '{}asistencia_{}_{}.csv'.format(self.salida_diaria,  today, kwargs["idgym"])
        contrato_nombre     =   '{}contrato_{}_{}.csv'.format(self.salida_diaria, today, kwargs["idgym"])

        #### ---- Guardamos en S3 como csv.gz ---- ####
        utils.save_csv_gz(self.listdict_to_dataframe(lista=self.usuario_info), info_nombre)
        utils.save_csv_gz(self.listdict_to_dataframe(lista=self.usuario_asistencia), asistencia_nombre)
        utils.save_csv_gz(self.listdict_to_dataframe(lista=self.usuario_contrato), contrato_nombre)

    def get_data(self, **kwargs):
        '''
        Realiza la consulta de los datos por cada usuario a DTG
        ------------------------------------------------------
        IN:
            kwargs["rut"]
        OUT:
            None
        '''

        #### ---- Generamos la data y hacemos la peticion ---- ####
        data        =   {"userid":kwargs["rut"]}
        headers     =   {"Authorization": self.token}
        solicitud   =   requests.get(self.url, params=data, headers=headers)

        if (solicitud.status_code    ==  200):
            response    =   solicitud.json()
            
            #### ---- Realizamos la separacion de los contratos ---- ####
            self.get_contratos(contratos=response["data"]["contract"], rut=response["data"]["id"])

            #### ---- Realizamos la separacion de las asistencias ---- ####
            self.get_asistencias(asistencia=response["data"]["assistance"], rut=response["data"]["id"])

            #### ---- Print informacion del usuario ---- ####
            self.get_informacion(usuario=response["data"])

        else:
            print("Ocurrio un error con", kwargs["rut"])
  
    def get_informacion(self, **kwargs):
        '''
        Obtiene solo la informacion del usuario y la guarda en el arreglo de usuario_info
        ---------------------------------------------------------------------------------
        IN:
            kwargs["usuario"]
        OUT:
            NONE
        '''

        #### ---- Asignamos y eliminamos las variables que no son de nuestro interes, en este caso ---- ####
        info_cruda             =   kwargs["usuario"]
        info_cruda["fecha_captura"]   =   self.fecha_hoy
        del info_cruda["assistance"]
        del info_cruda["contract"]

        #### ---- Guardamos en el arreglo ---- ####
        self.usuario_info.append(info_cruda)

    def get_contratos(self, **kwargs):
        '''
        Obtiene la informacion de los contratos y la guarda en usuario_contrato
        -----------------------------------------------------------------------
        IN:
            kwargs["contratos"]:    list-dict   |   Lista que contiene cada elemento como diccionario en donde aparece la informacion de los contratos
            kwargs["rut"]:          str         |   Rut del usuario
        OUT:
            NONE
        '''

        #### ---- Definimos los dos tipos de mostrar el contrato existentes ---- ####
        actual  =   "current"
        ultimos =   "last"

        #### ----- Operamos el contrato actual ---- ####
        #### ----- Agregamos el rut, luego agregamos a la lista mayor ---- ####
        try:
            json_contrato_actual                    =   kwargs["contratos"][actual]
            json_contrato_actual["rut"]             =   kwargs["rut"]
            json_contrato_actual["estadocontrato"]  =   "current"
            json_contrato_actual["fecha_captura"]   =   self.fecha_hoy
            self.usuario_contrato.append(json_contrato_actual)
        except Exception as E:
            #print(E)
            pass
            
        #### ----- Operamos los ultimos contratos ---- ####
        #### ---- Operamos los contratos ultimos ---- ####
        cantidad_ultimos_contrato   =   len(kwargs["contratos"][ultimos])

        if (cantidad_ultimos_contrato>0):
            for contrato in kwargs["contratos"][ultimos]:
                ultimo_contrato_json                    =   contrato
                ultimo_contrato_json["rut"]             =   kwargs["rut"]
                ultimo_contrato_json["estadocontrato"]  =   "last"
                ultimo_contrato_json["fecha_captura"]   =   self.fecha_hoy
                self.usuario_contrato.append(ultimo_contrato_json)

    def get_asistencias(self, **kwargs):
        '''
        Obtiene la informacion de la asistencia del usuario
        ----------------------------------------------------
        Parsea la informacion de asistencia, agrega el rut para tener el registro
        y guarda en self.usuario_asistencia

        IN:
            kwargs["asistencia"]:   list-dict   |   Lista que contiene cada elemento como diccionario en donde aparece la informacion de la asistencia
            kwargs["rut"]:          str         |   Rut del usuario
        OUT:
            NONE
        '''
        
        #### ---- Contamos la cantidad de asistencias y en caso de existir las recorremos ---- ####
        cantidad_de_asistencias =   len(kwargs["asistencia"])
        if cantidad_de_asistencias > 0:
            for asistencia in kwargs["asistencia"]:
                asistencia["rut"]   =   kwargs["rut"]
                asistencia["fecha_captura"]   =   self.fecha_hoy
                self.usuario_asistencia.append(asistencia)

    def save_csv(self, **kwargs):
        '''
        Guarda un archivo csv
        ----------------------
        IN:
            kwargs["lista"]:    list    | Lista donde cada uno de sus elementos es un diccionario
            kwargs["nombre"]:   str     | Nombre del archivo de salida
        OUT:
            Un archivo csv.
        '''
        try:
            df = pd.DataFrame(kwargs["lista"])
            df.to_csv(kwargs["nombre"])
        except Exception as e:
            print("Ocurrio algo al guardar el archivo", e)

    def listdict_to_dataframe(self, **kwargs):
        '''
        Transforma una lista en un dataframe
        ------------------------------------
        IN:
            kwargs["lista"]:    list        |   Lista donde cada uno de sus elementos es un diccionario
        OUT:
            df:                 dataframe   |   Dataframe de la lista de entrada
        '''
        try:
            df = pd.DataFrame(kwargs["lista"])
        except Exception as e:
            print("Ocurrio algo al guardar el archivo", e)
        
        return df

if __name__ == '__main__':
    idgym   =   sys.argv[1]
    print("INICIO: ",datetime.datetime.now())
    
    Capturemos  =   capture_data()
    Capturemos.recorremos_listablanca(idgym=idgym)
    
    Proceso.insertar(Proyecto="Sportlife", Proceso="Capturar Gym " + idgym,  Estado=1)
    
    print("FIN: ",datetime.datetime.now())