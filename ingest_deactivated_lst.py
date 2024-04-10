# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Configuration

# COMMAND ----------

!pip install openpyxl

# COMMAND ----------

from pathlib import Path
import pandas as pd
from datetime import datetime

path = "/Volumes/mimi_ws_1/nppes/src" # where all the input files are located
catalog = "mimi_ws_1" # delta table destination catalog
schema = "nppes" # delta table destination schema
table = "deactivated" 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Collect Files and File Headers to Ingest

# COMMAND ----------

files = []
for filepath in Path(f"{path}/{table}").glob("*"):
    yyyymmdd = filepath.stem[-8:]
    dt = datetime.strptime(yyyymmdd, "%Y%m%d").date()
    files.append((dt, filepath))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Ingestion

# COMMAND ----------

files_exist = {}
writemode = "overwrite"
# We want to skip those files that are already in the delta tables.
# We look up the table, and see if the files are already there or not.
if spark.catalog.tableExists(f"{catalog}.{schema}.{table}"):
    writemode = "append"
    files_exist = set([row["_input_file_date"] 
                   for row in 
                   (spark.read.table(f"{catalog}.{schema}.{table}")
                            .select("_input_file_date")
                            .distinct()
                            .collect())])
files_to_ingest = [item for item in files if item[0] not in files_exist]

# COMMAND ----------

for item in sorted(files_to_ingest, key=lambda x: x[0], reverse=True):
    pdf = pd.read_excel(str(item[1]), skiprows=1, converters={0: str})
    pdf.columns = ["npi", "deactivation_date"]
    pdf["deactivation_date"] = pd.to_datetime(pdf["deactivation_date"]).dt.date
    pdf["_input_file_date"] = item[0]
    (spark.createDataFrame(pdf)
        .write
        .format('delta')
        .mode(writemode)
        .saveAsTable(f"{catalog}.{schema}.{table}"))

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------


