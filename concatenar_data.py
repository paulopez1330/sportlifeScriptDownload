from datetime import date
import datetime
from numpy import inf
import pandas as pd
import utils
import sys
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

class Concatenar():
    #### ----- Ruta de los archivos de salida en S3 ----- ####
    salida_diaria       =   "s3://karrott-sporlife/raw/diario/"
    salida_diaria_gyms  =   "s3://karrott-sporlife/raw/diario/gyms/"
    

    def concatenar_gyms(self):
        '''
        Concatena la salia diaria de los gimnasios
        ------------------------------------------
        '''
        print(" Contanemos los archivos diarios por gimnasio")

        #### ---- Creamos los ids de los gimnasios ---- ####
        gyms    =   [x for x in range(1, 19)]
        gyms.append(39)
        gyms.append(40)

        #### ---- Utilizamos la fecha deel dia de hoy ---- ####
        today               =   date.today()
        today               =   today.strftime("%m_%d_%Y")
        
        #### ---- Logica para concatenar los df ---- ####
        for idgym in gyms:
            print("  Concantenando ", idgym)

            #### ---- Creamos los nombres ---- ####
            info_file         =   '{}info_{}_{}.csv.gz'.format(self.salida_diaria_gyms,  today, idgym)
            asistencia_file   =   '{}asistencia_{}_{}.csv.gz'.format(self.salida_diaria_gyms,  today, idgym)
            contrato_file     =   '{}contrato_{}_{}.csv.gz'.format(self.salida_diaria_gyms, today, idgym)

            try:
                if idgym == 1:
                    #### ---- Leemos el archivo ---- ####
                    df_info         =   pd.read_csv(info_file)
                    df_asistencia   =   pd.read_csv(asistencia_file)
                    df_contrato     =   pd.read_csv(contrato_file)
                else:
                    #### ---- Leemos el diario ---- ####
                    aux_df_info         =   pd.read_csv(info_file)
                    aux_df_asistencia   =   pd.read_csv(asistencia_file)
                    aux_df_contrato     =   pd.read_csv(contrato_file)
                    
                    #### ---- Concatenamos ---- ####
                    df_info         =   pd.concat([df_info, aux_df_info])
                    df_asistencia   =   pd.concat([df_asistencia, aux_df_asistencia])
                    df_contrato     =   pd.concat([df_contrato, aux_df_contrato])

            except Exception as e:
                print("Eror en gym", idgym, e)

        #### ---- Formateamos los nombres de salida ---- ####
        info_nombre_salida_diaria         =   '{}info_{}.csv.gz'.format(self.salida_diaria,  today)
        asistencia_nombre_salida_diaria   =   '{}asistencia_{}.csv.gz'.format(self.salida_diaria,  today)
        contrato_nombre_salida_diaria     =   '{}contrato_{}.csv.gz'.format(self.salida_diaria, today)

        print("  Escribiendo",info_nombre_salida_diaria)
        print("  Escribiendo",asistencia_nombre_salida_diaria)
        print("  Escribiendo",contrato_nombre_salida_diaria)

        #### ---- Volvemos a tener datos en blanco ---- ####
        df_contrato.loc[df_contrato['holder'].notna(), 'holder'] = df_contrato.loc[df_contrato['holder'].notna(), 'holder'].str.split('.').str[0]


        #### ---- Eliminamos duplicados ---- ####
        df_info         =   df_info.drop_duplicates()
        df_asistencia   =   df_asistencia.drop_duplicates()
        df_contrato     =   df_contrato.drop_duplicates()

        

        #### ---- Guardamos los archivos ---- ####
        utils.save_csv_gz(df_info, info_nombre_salida_diaria)
        utils.save_csv_gz(df_asistencia, asistencia_nombre_salida_diaria)
        utils.save_csv_gz(df_contrato, contrato_nombre_salida_diaria)
 

    def concatenar(self):
        print("\n Concatenando el dia en Historico")
        
        #### ---- Formateamos los nombres ---- ####
        today               =   date.today()
        today               =   today.strftime("%m_%d_%Y")
        info_nombre         =   '{}info_{}.csv.gz'.format(self.salida_diaria,  today)
        asistencia_nombre   =   '{}asistencia_{}.csv.gz'.format(self.salida_diaria,  today)
        contrato_nombre     =   '{}contrato_{}.csv.gz'.format(self.salida_diaria, today)

        #### ---- Leemos los archivos darios ---- ####
        df_info_hoy             =   pd.read_csv(info_nombre)
        df_asistencia_hoy       =   pd.read_csv(asistencia_nombre)
        df_contrato_hoy         =   pd.read_csv(contrato_nombre)
        
        #### ---- Leemos los archivos acumulados ---- ####
        df_info_acumulado       =   pd.read_csv("s3://karrott-sporlife/raw/historico/info_todos_acumulado.csv.gz")
        df_asistencia_acumulado =   pd.read_csv("s3://karrott-sporlife/raw/historico/asistencia_todos_acumulado.csv.gz")
        df_contrato_acumulado   =   pd.read_csv("s3://karrott-sporlife/raw/historico/contrato_todos_acumulado.csv.gz")

        #### ---- Concatenamos los historicos con los actuales ---- ####
        concatenamos_info           =   pd.concat([df_info_hoy, df_info_acumulado], axis=0)
        concatenamos_asistencia     =   pd.concat([df_asistencia_hoy, df_asistencia_acumulado], axis=0)
        concatenamos_contrato       =   pd.concat([df_contrato_hoy, df_contrato_acumulado], axis=0)

        #### ---- Borramos los duplicados ---- ####
        concatenamos_info           =   concatenamos_info.drop_duplicates()
        concatenamos_asistencia     =   concatenamos_asistencia.drop_duplicates()
        concatenamos_contrato       =   concatenamos_contrato.drop_duplicates()

        #### ---- Guardamos los archivos como csv.gz en S3 ---- ####
        utils.save_csv_gz(concatenamos_info, "s3://karrott-sporlife/raw/historico/info_todos_acumulado.csv")
        utils.save_csv_gz(concatenamos_asistencia, "s3://karrott-sporlife/raw/historico/asistencia_todos_acumulado.csv")
        utils.save_csv_gz(concatenamos_contrato, "s3://karrott-sporlife/raw/historico/contrato_todos_acumulado.csv")
        

if __name__ == '__main__':
    print("INICIO: ",datetime.datetime.now())

    Concatenar  =   Concatenar()
    Concatenar.concatenar_gyms()
    Concatenar.concatenar()

    Proceso.insertar(Proyecto="Sportlife", Proceso="Concatenar Informacion",  Estado=1)

    print("FIN: ",datetime.datetime.now())