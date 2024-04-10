# Databricks notebook source
dbutils.widgets.dropdown("tablename", 
                         "endpoint", 
                         ["endpoint", "othername", "pl"])

# COMMAND ----------

!pip install tqdm

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Configuration

# COMMAND ----------

from pathlib import Path
import re
from tqdm import tqdm
import csv
from pyspark.sql.types import StructType, StructField, StringType, DateType
from pyspark.sql.functions import col, lit
from datetime import datetime

path = "/Volumes/mimi_ws_1/nppes/src" # where all the input files are located
catalog = "mimi_ws_1" # delta table destination catalog
schema = "nppes" # delta table destination schema
tablename = dbutils.widgets.get("tablename") # destination table

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Collect Files and File Headers to Ingest

# COMMAND ----------

file_headers = []
files = []
for filepath in Path(f"{path}/{tablename}_header").glob("*"):
    yyyymmdd = re.search(r"(\d{8})-(\d{8})", filepath.stem).group(2)
    dt = datetime.strptime(yyyymmdd, "%Y%m%d").date()
    file_headers.append((dt, filepath))
for filepath in Path(f"{path}/{tablename}").glob("*"):
    yyyymmdd = re.search(r"(\d{8})-(\d{8})", filepath.stem).group(2)
    dt = datetime.strptime(yyyymmdd, "%Y%m%d").date()
    files.append((dt, filepath))

file_headers = sorted(file_headers, key=lambda x: x[0], reverse=True)
data_header = next(csv.reader(open(file_headers[0][1], "r")))
table_schema = StructType([StructField(re.sub(r'\W+', '', column.lower().replace(' ','_')), 
                                 StringType(), 
                                 True) 
                        for column in data_header])

# COMMAND ----------

files_exist = {}
# We want to skip those files that are already in the delta tables.
# We look up the table, and see if the files are already there or not.

#writemode = "overwrite"
writemode = "append" # just to be safe...

if spark.catalog.tableExists(f"{catalog}.{schema}.{tablename}"):
    writemode = "append"
    files_exist = set([row["_input_file_date"] 
                   for row in 
                   (spark.read.table(f"{catalog}.{schema}.{tablename}")
                            .select("_input_file_date")
                            .distinct()
                            .collect())])
files_to_ingest = [item for item in files if item[0] not in files_exist]

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Ingestion

# COMMAND ----------

for item in tqdm(sorted(files_to_ingest, key=lambda x: x[0], reverse=True)):
    df = (spark.read.format("csv")
            .option("header", "false")
            .option("skipRows", "1")
            .schema(table_schema)
            .load(str(item[1]))
            .withColumn('_input_file_date', lit(item[0])))
    
    (df.write
        .format('delta')
        .mode(writemode)
        .saveAsTable(f"{catalog}.{schema}.{tablename}"))

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

files_to_ingest

# COMMAND ----------


