import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import xarray as xr
import numpy as np
import netCDF4
import os
import datetime
from datetime import date

#PRE-REQUISITI
# I file devono essere caricati sul bucket s3 all'indirizzo specificato
# Questo script è stato utilizzato per la conversione dei file all'interno di una macchina EC2

BUCKET_NAME = '*******'
s3 = boto3.resource("s3")
s3Client = boto3.client('s3')
bucket = s3.Bucket(BUCKET_NAME)
base_path = os.getcwd()
download_path = base_path + '/'
variables = ['rh', 'ssrd', 'tp', 't2m']
var_eobs =['pp','rr','tn','tx','tg']
datasets = ['era5','e-obs','ecmwf']
second_bucket_path = '{}/o/avg/'
parquet_path= '{}/p/parquet/{}/'


def conversion():
    try:
        for dataset in datasets:
            second_bucket_path = second_bucket_path.format(dataset)
            objs = bucket.objects.filter(Delimiter = '/', Prefix=second_bucket_path)
            for obj in objs:
                name_of_file = obj.key.split('/')[-1]
                if 'grib' in name_of_file:
                    engine = 'cfgrib'
                    rep = 'grib'
                    decode_times = False
                elif 'nc' in name_of_file:
                    var = [x for x in variables if x in name_of_file][0]
                    engine = 'netcdf4'
                    rep = 'nc'
                    decode_times = True
                    bucket.download_file(obj.key, download_path+name_of_file)
                    f = open(download_path+name_of_file, 'r')
                    data = xr.open_dataset(name_of_file, engine=engine, decode_times=decode_times)
                    if(decode_times):
                        data['time'] = data.indexes['time'].normalize().astype(str)
                    data = data.to_dataframe()
                    name = name_of_file.replace(rep, 'parquet')
                    data.to_parquet(name, engine='fastparquet', compression='gzip')
                    table2 = pq.read_table('./'+name)
                    bucket.upload_file(download_path+name, parquet_path.format(dataset, var)+name)
                    object = s3.Bucket(BUCKET_NAME).Object(parquet_path.format(dataset, var)+name)
                    object.Acl().put(ACL='public-read')
                    os.remove(download_path+name_of_file)
                    os.remove(download_path+name)
    except Exception as e:
        print(e)

if __name__ == "__main__":
    conversion()
