# Databricks notebook source
dbutils.widgets.dropdown("tablename", 
                         "endpoint", 
                         ["endpoint", "npidata", "othername", "pl"])

# COMMAND ----------

from pathlib import Path
import re
import dlt
import pandas as pd
import csv
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col

path = "/Volumes/mimi_ws_1/nppes/src"
catalog = "mimi_ws_1"
schema = "nppes"
tablename = dbutils.widgets.get("tablename")

# COMMAND ----------

file_headers = []
files = []
for filepath in Path(f"{path}/{tablename}_header").glob("*"):
    yyyymmdd = re.search(r"(\d{8})-(\d{8})", filepath.stem).group(2)
    file_headers.append((yyyymmdd, filepath))
for filepath in Path(f"{path}/{tablename}").glob("*"):
    yyyymmdd = re.search(r"(\d{8})-(\d{8})", filepath.stem).group(2)
    files.append((yyyymmdd, filepath))

file_headers = sorted(file_headers, key=lambda x: x[0], reverse=True)
data_header = next(csv.reader(open(file_headers[0][1], "r")))
table_schema = StructType([StructField(re.sub(r'\W+', '', column.lower().replace(' ','_')), 
                                 StringType(), 
                                 True) 
                        for column in data_header] + 
                    [StructField('_input_file_name', StringType(), True)])

# COMMAND ----------


files_exist = {}
if spark.catalog.tableExists(f"{catalog}.{schema}.{tablename}"):
    files_exist = set([row["_input_file_name"] 
                   for row in 
                   (spark.read.table(f"{catalog}.{schema}.{tablename}")
                            .select("_input_file_name")
                            .distinct()
                            .collect())])

# COMMAND ----------

files_to_ingest = [item for item in files if item[1].name not in files_exist]

# COMMAND ----------

df = (spark.read.format("csv")
            .option("header", "false")
            .option("skipRows", "1")
            .schema(table_schema)
            .load([str(item[1]) for item in files_to_ingest])
            .withColumn('_input_file_name', col("_metadata.file_name")))

# COMMAND ----------

writemode = "overwrite"
if spark.catalog.tableExists(f"{catalog}.{schema}.{tablename}"):
    writemode = "append"
df.write.mode(writemode).saveAsTable(f"{catalog}.{schema}.{tablename}")

# COMMAND ----------


