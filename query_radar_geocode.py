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

catalog = "mimi_ws_1"
schema = "nppes"
table_in = "address_key"
table_out = "address_radar"
max_cnt = 25000 # max processing volume per run; we do this to limit the spend
mdb_cnt = 100 # mdb insert many volume
radar_key_name = "RADAR_LIVE_KEY2"
radar_live_key = dbutils.secrets.get(scope="radar", key=radar_key_name)
mdb_dataapi_key = dbutils.secrets.get(scope="mdb", key="MDB_DATAAPI_KEY")
url = "https://api.radar.io/v1/geocode/forward"
headers = {"authorization": radar_live_key}

# COMMAND ----------

def push_to_mdb(documents):
    url = "https://data.mongodb-api.com/app/data-izvyi/endpoint/data/v1/action/insertMany"
    payload = json.dumps({
        "collection": "address_radar",
        "database": "nppes",
        "dataSource": "mimi2",
        "documents": documents
    })
    headers = {
        'Content-Type': 'application/json',
        'Access-Control-Request-Headers': '*',
        'api-key': mdb_dataapi_key,
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    return response.text

def reset_mdb():
    url = "https://data.mongodb-api.com/app/data-izvyi/endpoint/data/v1/action/deleteMany"
    payload = json.dumps({
        "collection": "address_radar",
        "database": "nppes",
        "dataSource": "mimi2",
        "filter": {}
    })
    headers = {
        'Content-Type': 'application/json',
        'Access-Control-Request-Headers': '*',
        'api-key': mdb_dataapi_key,
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    return response.text

# COMMAND ----------

df_out = None
if spark.catalog.tableExists(f"{catalog}.{schema}.{table_out}"):
    df_out = (spark.read.table(f"{catalog}.{schema}.{table_out}")
              .select("address_key",
                      "last_updated"))

# COMMAND ----------

df_in = (spark.read.table(f"{catalog}.{schema}.{table_in}")
            .filter(col("address_key") != "|||||"))

# COMMAND ----------

if df_out is not None:
    df_in = (df_in.join(df_out, 
                        on=["address_key"],
                        how="left")
                    .filter(col("last_updated").isNull()))
    
df_in = df_in.orderBy(desc(col("npi_b1_cnt")))

# COMMAND ----------

pdf = df_in.limit(max_cnt).toPandas()

# COMMAND ----------

data = []
mdb_docs = []
for _, row in tqdm(pdf.iterrows()):
    address_key = row["address_key"]
    line1, line2, city, state, zipcode, country = address_key.split("|")
    line = f"{line1} {line2}".strip()
    address = f"{line}, {city}, {state} {zipcode[:5]}".strip()
    payload = {"query": address,
            "country": "US"}
    try: 
        r = requests.get(url, headers=headers, params=payload)
    except requests.exceptions.RequestException as e:
        print(row, e)
    if r.status_code == 200:
        rjson = r.json()
        meta = rjson["meta"]
        matches = r.json()["addresses"]
        match_top = {}
        if len(matches) > 0:
            match_top = matches[0]
        d = [address_key,
                match_top.get("confidence"),
                match_top.get("latitude"), 
                match_top.get("longitude"),
                match_top.get("number"),
                match_top.get("street"), 
                match_top.get("city"), 
                match_top.get("county"),
                match_top.get("stateCode"),
                match_top.get("postalCode"), 
                match_top.get("countryCode"),
                match_top.get("formattedAddress"),
                match_top.get("addressLabel"),
                len(matches), 
                datetime.now()]
        data.append(d)
        # MongoDB is our backup
        mdb_docs.append({"address_key": address_key, "matches": matches})
        if len(mdb_docs) > mdb_cnt:
            push_to_mdb(mdb_docs)
            mdb_docs = []
if len(mdb_docs) > 0:
    push_to_mdb(mdb_docs)

# COMMAND ----------

columns = ["address_key",
           "radar_confidence", "radar_latitude", "radar_longitude", 
           "radar_number", "radar_street", "radar_city", "radar_county", 
           "radar_state_code", "radar_postal_code", "radar_country_code", 
           "radar_formatted_address", "radar_address_label", 
           "radar_cnt_matches", "last_updated"]

# COMMAND ----------

#writemode = "overwrite" if df_out is None else "append"
writemode = "append" # to make it safe moving forward

(spark.createDataFrame(pd.DataFrame(data, columns=columns))
        .write
        .format('delta')
        .mode(writemode)
        .saveAsTable(f"{catalog}.{schema}.{table_out}"))

# COMMAND ----------


