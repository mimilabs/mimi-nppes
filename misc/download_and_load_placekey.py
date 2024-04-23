# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Placekey Augmentation
# MAGIC
# MAGIC https://www.placekey.io/blog/joining-overture-and-npi-datasets
# MAGIC
# MAGIC This data is used to augment the NPPES provider addresses.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download the files on the blog post

# COMMAND ----------

!pip install tqdm

# COMMAND ----------

import requests
from pathlib import Path
from tqdm import tqdm
import zipfile
import csv
import re
import json
import pandas as pd
from datetime import datetime
from pyspark.sql.functions import lit

# COMMAND ----------

url = ("https://safegraph-public.s3.us-west-2.amazonaws.com/"
       "placekey/blog/joining_overture_and_npi_datasets_with_placekey")
volumepath = "/Volumes/mimi_ws_1/nppes/src/placekey"
files_to_download = ["us_overture_placekey_skinny_file.csv",
                    "npi_to_placekey_skinny_file.csv",
                    "full_overture_with_placekey.csv",
                    "full_npi_with_placekey.csv",
                    "full_npi_to_overture_joined.csv"]
catalog = "mimi_ws_1"
schema = "nppes"
table = "address_placekey"

# COMMAND ----------

def download_file(url, filename, folder):
    # NOTE the stream=True parameter below
    with requests.get(f"{url}/{filename}", stream=True) as r:
        r.raise_for_status()
        with open(f"{folder}/{filename}", 'wb') as f:
            for chunk in tqdm(r.iter_content(chunk_size=8192)): 
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk: 
                f.write(chunk)

# COMMAND ----------

# Display the zip file links
for filename in files_to_download:
    # Check if the file exists
    if Path(f"{volumepath}/{filename}").exists():
        # print(f"{filename} exists, skipping...")
        continue
    else:
        print(f"{filename} downloading...")
        download_file(url, filename, volumepath)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest the files into Detal tables

# COMMAND ----------

vars_to_keep = ["NPI", 
                "id",
                "placekey",
                "address_placekey",
                "building_placekey",
                "geometry",
                "names",
                "categories",
                "confidence", 
                "websites",
                "socials",
                "emails",
                "phones",
                "addresses"]
data = []
with open(f"{volumepath}/full_npi_to_overture_joined.csv") as fp:
    reader = csv.reader(fp,escapechar='\\')
    header = next(reader)
    for row in tqdm(reader):
        doc = {k: d for k, d in zip(header, row)}
        row2 = [doc[k] for k in vars_to_keep]
        if row2[1] == "":
            continue
        long, lat = None, None
        geocode = re.findall(r"-?\d+\.\d+", row2[5])
        if len(geocode) == 2:
            try:
                long, lat = float(geocode[0]), float(geocode[1])
            except ValueError:
                long, lat = None, None
        primary_name = ""
        if row2[6] != "":
            primary_name = json.loads(row2[6]).get("primary")
        categories = {"main": "", "alternate": []}
        if row2[7] != "":
            categories = json.loads(row2[7])
        category_main = categories.get("main")
        category_others = "; ".join(categories.get("alternate", []))
        try:
            confidence = float(row2[8])
        except ValueError:
            confidence = None
        website = row2[9][1:-1] if len(row2[9]) > 0 else ""
        social = row2[10][1:-1] if len(row2[10]) > 0 else ""
        email = row2[11][1:-1] if len(row2[11]) > 0 else ""
        phone = row2[12][1:-1] if len(row2[12]) > 0 else ""
        if phone[:2] != "+1":
            phone = "+1" + phone
        address = {}
        if row2[13] != "":
            address = json.loads(row2[13])[0]
        address_key = "|".join([address.get("freeform", "").upper().split(",")[0], 
                                "",
                        address.get("locality", "").upper(), 
                        address.get("region", "").upper(),
                        address.get("postcode", "")[:5],
                        address.get("country", "").upper()])
        row3 = [row2[0], row2[1], row2[2], row2[3], row2[4],
                lat, long, primary_name, 
                category_main, category_others, 
                confidence, 
                website, social, email, phone, 
                address_key]
        data.append(row3)

# COMMAND ----------

columns = ["npi", "overture_id", "placekey", "address_placekey", "building_placekey",
                    "pk_latitude", "pk_longitude", "pk_primary_name", 
                    "pk_category_main", "pk_category_others", 
                    "pk_confidence", 
                    "pk_website", "pk_social", "pk_email", "pk_phone", 
                    "pk_address_key"]
df = spark.createDataFrame(pd.DataFrame(data, columns=columns))
df = df.withColumn("last_updated", lit(datetime.now()))

# COMMAND ----------

df.display()

# COMMAND ----------

(df.write.format("delta")
        .mode("overwrite")
        .saveAsTable(f"{catalog}.{schema}.{table}"))

# COMMAND ----------


