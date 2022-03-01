# -*- coding: utf-8 -*-
"""
Created on Tue Feb 16 10:55:33 2021

@author: Jorge
"""
import io
import pandas as pd
import os
import pdb
import datetime as dt
import boto3
import gzip
from io import BytesIO, TextIOWrapper
from urllib.parse import urlparse
import warnings
warnings.simplefilter('always')


def create_folder_if_not_exists(path):
    #creamos el directorio de salida en caso que no exista previamente
    if 's3://' not in path:    #si la salida no es s3, chequeamos si hay que crear algun directorio
        directory = os.path.dirname(path)
        if directory == '':
            pass
        else:
            if not os.path.exists(directory):
                os.makedirs(directory)

def save_csv(df, file_path, index=True):
    if 's3://' not in file_path:    #si la salida no es s3, chequeamos si hay que crear algun directorio
        directory = os.path.dirname(file_path)
        if directory == '':
            pass
        else:
            if not os.path.exists(directory):
                os.makedirs(directory)
    df.to_csv(file_path, index=index)

def save_pickle(df, file_path):
    if 's3://' not in file_path:    #si la salida no es s3, chequeamos si hay que crear algun directorio
        directory = os.path.dirname(file_path)
        if directory == '':
            pass
        else:
            if not os.path.exists(directory):
                os.makedirs(directory)
    df.to_pickle(file_path)

def save_csv_gz(df, file_path, index=False):
    #chequeamos si tiene extension .gz
    if file_path[-3:]!='.gz':
        #print('Se agrega extension ".gz" a {:s}'.format(file_path))
        file_path = file_path + '.gz'
    if 's3://' not in file_path:    #si la salida no es s3, chequeamos si hay que crear algun directorio
        directory = os.path.dirname(file_path)
        if directory == '':
            pass
        else:
            if not os.path.exists(directory):
                os.makedirs(directory)
        df.to_csv(file_path, compression='gzip', index=index)
    else: #si hay que guardar en s3
        #obtenemos bucket y path
        o = urlparse(file_path, allow_fragments=False)
        bucket_name = o.netloc
        in_bucket_path = o.path.lstrip('/')
        
        gz_buffer = BytesIO()
        with gzip.GzipFile(mode='w', fileobj=gz_buffer) as gz_file:
            df.to_csv(TextIOWrapper(gz_file, 'utf8'), index=index)
        s3_resource = boto3.resource('s3')
        s3_object = s3_resource.Object(bucket_name, in_bucket_path)
        s3_object.put(Body=gz_buffer.getvalue())
    print('Archivo {:s} guardado exitosamente.'.format(file_path))

def save_pickle_bz2(df, file_path, index=True):
    #chequeamos si tiene extension .bz2
    if file_path[-4:]!='.bz2':
        print('Se agrega extension ".bz2" a {:s}'.format(file_path))
        file_path = file_path + '.bz2'
    if 's3://' not in file_path:    #si la salida no es s3, chequeamos si hay que crear algun directorio
        directory = os.path.dirname(file_path)
        if directory == '':
            pass
        else:
            if not os.path.exists(directory):
                os.makedirs(directory)
        df.to_pickle(file_path)
    else: #si hay que guardar en s3
        #obtenemos bucket y path
        o = urlparse(file_path, allow_fragments=False)
        bucket_name = o.netloc
        in_bucket_path = o.path.lstrip('/')
        
        pickle_buffer = io.BytesIO()
        df.to_pickle(pickle_buffer, compression='bz2')
        s3_resource = boto3.resource('s3')
        s3_object = s3_resource.Object(bucket_name, in_bucket_path)
        s3_object.put(Body=pickle_buffer.getvalue())
    print('Archivo {:s} guardado exitosamente.'.format(file_path))

def update_invalid_list(lista_invalidos, reset=False):
    if not isinstance(lista_invalidos, list):
        raise Exception('lista_invalidos debe ser tipo list. Se recibio {:s}'.format(str(type(lista_invalidos))))        
        
    #descargamos lista actual
    df_old = pd.read_csv('s3://karrott-extras/cleaning/lista_correos_invalidos.csv.gz')
    #y hacemos respaldo
    save_csv_gz(df_old, 's3://karrott-extras/cleaning/lista_correos_invalidos_resp.csv', index=False)

    #creamos df con lista nueva
    df = pd.DataFrame(lista_invalidos, columns=['e_mail'])
    df['fecha_update'] = str(dt.date.today())
    if reset:
        save_csv_gz(df, 's3://karrott-extras/cleaning/lista_correos_invalidos.csv', index=False)
        print('Se reseteo la lista de correos invalidos')
    else:
        #conservamos los que no estan en la lista antigua
        df = df[~df.e_mail.isin(df_old.e_mail)].copy()
        largo = len(df)
        if len(df)>0:
            df = pd.concat([df_old, df])
            save_csv_gz(df, 's3://karrott-extras/cleaning/lista_correos_invalidos.csv', index=False)
            print('Lista de correos invalidos actualizada. Se agregaron {:d} registros.'.format(largo))
        else:
            print('No hay correos nuevos para agregar a lista de correos invalidos')

def get_invalid_list():
    #descargamos lista actual
    df = pd.read_csv('s3://karrott-extras/cleaning/lista_correos_invalidos.csv.gz')    
    return df.e_mail.to_list()
