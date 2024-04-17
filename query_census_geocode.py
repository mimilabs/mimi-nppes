# Databricks notebook source
!pip install tqdm

# COMMAND ----------

import requests
from pyspark.sql.functions import col, desc
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import pandas as pd
import json
from tqdm import tqdm
import csv

catalog = "mimi_ws_1"
schema = "nppes"
table_in = "address_key"
table_out = "address_census"
batch_cnt = 5000 # max processing volume per run; we do this to limit the spend
url = "https://geocoding.geo.census.gov/geocoder/locations/addressbatch"

# COMMAND ----------

def clean_street_address(street):
    # https://andrewpwheeler.com/2021/02/09/geocoding-the-cms-npi-registry-python/
    end_str = [' STE', ' SUITE', ' BLDG', ' TOWER', ', #', ' UNIT',
           ' APT', ' BUILDING', ',', '#']
    street_new = street.upper()
    for su in end_str:
        sf = street.find(su)
        if sf > -1:
            street_new = street_new[0:sf]
    street_new = (street_new.replace(".","")
                            .replace(",", "")
                            .replace("&", ""))
    street_new = street_new.strip()
    return street_new

# COMMAND ----------

df_out = None
writemode = "overwrite"
if spark.catalog.tableExists(f"{catalog}.{schema}.{table_out}"):
    writemode = "append"
    df_out = (spark.read.table(f"{catalog}.{schema}.{table_out}")
              .select("address_key",
                      "last_updated"))
    
df_in = (spark.read.table(f"{catalog}.{schema}.{table_in}")
            .filter(col("address_key") != "|||||"))
if df_out is not None:
    df_in = (df_in.join(df_out, 
                        on=["address_key"],
                        how="left")
                    .filter(col("last_updated").isNull()))
    
df_in = df_in.orderBy(desc(col("npi_b1_cnt")))
pdf = df_in.limit(batch_cnt).toPandas()

# COMMAND ----------

while pdf.shape[0] > 0:
    data = []
    keymap = {}
    for i, (_, row) in enumerate(pdf.iterrows()):
        address_key = row["address_key"]
        keymap[str(i)] = address_key
        line1, line2, city, state, zipcode, country = address_key.split("|")
        line = clean_street_address(f"{line1} {line2}".strip())
        data.append(",".join([str(i), line, city, state, zipcode[:5]]))
    res = requests.post(url, files={"addressFile": ("inputfile.csv", "\n".join(data), "text/csv")}, 
                            data={"benchmark": '4'})

    reader = csv.reader(res.text.split("\n"))
    data_census = []
    for row in reader:
        if len(row) == 0:
            continue
        lat, long = None, None
        addr_clean = None
        confidence = None
        if len(row) > 5 and len(row[5].split(',')) == 2:
            try:
                lat = float(row[5].split(',')[1])
                long = float(row[5].split(',')[0])
            except ValueError:
                lat = None
                long = None
        if len(row) > 4: 
            addr_clean = row[4]
        if len(row) > 3:
            confidence = row[3]
        data_census.append([keymap[row[0]],
                            row[2], 
                            confidence,
                            addr_clean,
                            lat, long, 
                            datetime.now()])
    pdf = pd.DataFrame(data_census, columns=["address_key", 
                                    "census_status", 
                                    "census_confidence", 
                                    "census_address", 
                                    "census_latitude",
                                    "census_longitude",
                                    "last_updated"])
    (spark.createDataFrame(pdf).write
            .format('delta')
            .mode(writemode)
            .saveAsTable(f"{catalog}.{schema}.{table_out}"))
    
    # Update the batch data
    writemode = "append"
    df_out = (spark.read.table(f"{catalog}.{schema}.{table_out}")
                .select("address_key",
                        "last_updated"))    
    df_in = (spark.read.table(f"{catalog}.{schema}.{table_in}")
                .filter(col("address_key") != "|||||")
                .join(df_out, on=["address_key"], how="left")
                .filter(col("last_updated").isNull()))
    df_in = df_in.orderBy(desc(col("npi_b1_cnt")))
    pdf = df_in.limit(batch_cnt).toPandas()

# COMMAND ----------


