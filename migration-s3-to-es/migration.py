import boto3
from tqdm import tqdm
import json
from joblib import Parallel, delayed
import multiprocessing
import requests
from io import StringIO
import pandas as pd
import time
import datetime


def import_obj(key, elastic_endpoint, bucket):
    s3 = boto3.client("s3")
    path_as_list = key.split("/")
    path = "-".join(path_as_list[:-1]).lower()
    file = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode('utf-8')

    df = pd.read_csv(StringIO(file), sep=",")

    if len(df.index) == 0:
        return

    request = requests.request("PUT", url=elastic_endpoint+"/"+path, data=json.dumps({}), headers={"Content-Type": "application/json"})

    body = {
        "data": {
            "properties": {
                "latitude": {"type": "float"},
                "longitude": {"type": "float"},
                "value": {"type": "float"}
            }
        }
    }

    request = requests.request("PUT", url=f"{elastic_endpoint}/{path}/_mapping/data", data=json.dumps(body), headers={"Content-Type": "application/json"})

    min_latitude = df["latitude"].min()
    min_longitude = df["longitude"].min()
    max_latitude = df["latitude"].max()
    max_longitude = df["longitude"].max()

    body = {
        "data": {
            "properties": {
                "max_latitude": {"type": "float"},
                "min_latitude": {"type": "float"},
                "max_longitude": {"type": "float"},
                "min_longitude": {"type": "float"}
            }
        }
    }

    request = requests.request("PUT", url=f"{elastic_endpoint}/{path}-boundaries/_mapping/boundaries", data=json.dumps(body), headers={"Content-Type": "application/json"})

    body = {
        "max_latitude": max_latitude,
        "min_latitude": min_latitude,
        "max_longitude": max_longitude,
        "min_longitude": min_longitude
    }

    request = requests.request("POST", url=f"{elastic_endpoint}/{path}-boundaries/boundaries", data=json.dumps(body),headers={"Content-Type": "application/json"})

    file = file.split("\n")
    for line in file[1::]:
        if not line == "":
            info = line.replace('"', '').split(",")
            body = {
                "longitude": float(info[0]),
                "latitude": float(info[1]),
                "value": float(info[2])
            }
            request = requests.request("POST", url=f"{elastic_endpoint}/{path}/data", data=json.dumps(body), headers={"Content-Type": "application/json"})


if __name__ == "__main__":
    with open("./config.json", "r") as fp:
        info = json.load(fp)
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(info["bucket"])
    objects = list()

    for file in bucket.objects.all():
            if file.key.split(".")[-1] == "csv":
                objects.append(file.key)

    num_cores = multiprocessing.cpu_count()

    print(f"Importing using {num_cores} threads")

    start = int(round(time.time() * 1000))
    print(f"Import process start at {datetime.datetime.now()}")
    Parallel(n_jobs=num_cores)(delayed(import_obj)(key, info['elastic_endpoint'], info["bucket"]) for key in tqdm(objects))
    end = int(round(time.time() * 1000))
    print(f"Import process ended at {datetime.datetime.now()}")
    print(f"Migration took: {(end-start)/1000} seconds")
