# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # NPI Data Ingestion
# MAGIC
# MAGIC The NPI data files are too big. Even for Databricks, the size is rather challenging to handle. 
# MAGIC
# MAGIC One way to handle such a big file is to break it into smaller chunks, and ingest chunk by chunk. 
# MAGIC
# MAGIC Also, we compress some redundant columns to reduce the size.
# MAGIC
# MAGIC The script below does that. 

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
from pyspark.sql.functions import col, to_date
from datetime import datetime
from collections import defaultdict

path = "/Volumes/mimi_ws_1/nppes/src"
catalog = "mimi_ws_1"
schema = "nppes"
tablename = "npidata"
block_size = 250000

# COMMAND ----------

# MAGIC %md
# MAGIC ## Collect Files to Ingest

# COMMAND ----------

files = []
for filepath in Path(f"{path}/{tablename}").glob("*"):
    yyyymmdd = re.search(r"(\d{8})-(\d{8})", filepath.stem).group(2)
    dt = datetime.strptime(yyyymmdd, "%Y%m%d").date() # this is the date when the file was created
    files.append((dt, filepath))
files = sorted(files, key=lambda x: x[0], reverse=True)

block_exist = {}
for filepath in Path(f"{path}/{tablename}block").glob("*"):
    block_exist[filepath.stem.split("_")[0]] = 1

# COMMAND ----------

# The columns that match these regex patterns are sparsely populated.
# Technically, they are in an array form, but they are put in a wide format.
# We group them into a single column for each group.
groupings = {
        "healthcare_provider_taxonomy_code_(\d+)": ("healthcare_provider_taxonomies", 0),
        "provider_license_number_(\d+)": ("healthcare_provider_taxonomies", 1),
        "provider_license_number_state_code_(\d+)": ("healthcare_provider_taxonomies", 2),
        "healthcare_provider_primary_taxonomy_switch_(\d+)": ("healthcare_provider_taxonomies", 3),
        "other_provider_identifier_(\d+)": ("other_provider_identifiers", 0),
        "other_provider_identifier_type_code_(\d+)": ("other_provider_identifiers", 1),
        "other_provider_identifier_state_(\d+)": ("other_provider_identifiers", 2),
        "other_provider_identifier_issuer_(\d+)": ("other_provider_identifiers", 3),
        "healthcare_provider_taxonomy_group_(\d+)": ("healthcare_provider_taxonomy_groups", 0)
    }

def get_compress_index(header):
    compress_index = {}
    header_comp = []
    comp_groups = []
    for i, column in enumerate(header):
        for pattern, loc in groupings.items():
            search_res = re.search(pattern, column)
            if search_res == None:
                continue
            compress_index[i] = (loc[0], int(search_res.group(1)),loc[1])
            if loc[0] not in comp_groups:
                comp_groups.append(loc[0]) 
        if i not in compress_index:
            header_comp.append(column)
    header_comp += comp_groups
    return compress_index, header_comp, comp_groups

def compress_row(header, row, compress_index, comp_groups):
    row_comp = []
    comp_targets = {group: defaultdict(list) for group in comp_groups}
    for i, (k, v) in enumerate(zip(header, row)):
        if i in compress_index:
            col_grp = compress_index[i][0]
            col_sub = compress_index[i][1]
            comp_targets[col_grp][col_sub].append(v)
        elif k[-5:] == "_date":
            # We change the date representation
            try:
                row_comp.append(datetime
                                 .strptime(v,"%m/%d/%Y")
                                 .strftime("%Y-%m-%d"))
            except ValueError:
                row_comp.append("")
        else:
            row_comp.append(v.strip())
    for col_grp in comp_groups:
        items = comp_targets[col_grp]
        subgroups = []
        for col_sub, val_sub in items.items():
            if all(x.strip()=="" for x in val_sub):
                continue
            subgroups.append('^'.join(val_sub))
        if col_grp == "healthcare_provider_taxonomies":
            # we put the "primary" taxonomy first, and then the rest 
            subgroups = sorted(subgroups, key=lambda x: int(x[-1]!="Y"))
        row_comp.append("|".join(subgroups))
    return row_comp

# COMMAND ----------

# MAGIC %md
# MAGIC ## Make Small Chunks (Blocks)

# COMMAND ----------

for item in tqdm(files):
    
    if item[0].strftime("%Y-%m-%d") in block_exist:
        continue
    
    block = []
    block_cnt = 0 # block number reets for each input file
    
    with open(item[1], "r") as fp_in:
        reader = csv.reader(fp_in)
        header = next(reader)
        header = [re.sub(r'\W+', '', column.lower().replace(' ','_'))
                         for column in header]
        compress_index, header_comp, comp_groups = get_compress_index(header)
        input_file_dt = item[0].strftime("%Y-%m-%d")
        for row in reader:
            row_comp = compress_row(header, row, 
                                    compress_index,
                                    comp_groups)
            block.append(row_comp + [input_file_dt])
            if len(block) > block_size:
                block_cnt += 1
                fn_block = f"{path}/{tablename}block/{input_file_dt}_{block_cnt:03}.csv"
                with open(fn_block, "w") as fp_out:
                    print(f"{fn_block}...")
                    writer = csv.writer(fp_out)
                    writer.writerow(header_comp + ["_input_file_date"])
                    writer.writerows(block)
                    block = []
        if len(block) > 0:
            block_cnt += 1
            fn_block = f"{path}/{tablename}block/{input_file_dt}_{block_cnt:03}.csv"
            with open(fn_block, "w") as fp_out:
                print(f"{fn_block}...")
                writer = csv.writer(fp_out)
                writer.writerow(header_comp + ["_input_file_date"])
                writer.writerows(block)
                block = []

# COMMAND ----------

files_exist = {}
files_to_ingest = []

# We want to skip those files that are already in the delta tables.
# We look up the table, and see if the files are already there or not.
if spark.catalog.tableExists(f"{catalog}.{schema}.{tablename}"):
    files_exist = set([row["_input_file_date"] 
                   for row in 
                   (spark.read.table(f"{catalog}.{schema}.{tablename}")
                            .select("_input_file_date")
                            .distinct()
                            .collect())])

for filepath in Path(f"{path}/{tablename}block").glob("*"):
    yyyymmdd = re.search(r"(\d{4}-\d{2}-\d{2})_(\d{3})", filepath.stem).group(1)
    dt = datetime.strptime(yyyymmdd, "%Y-%m-%d").date()
    if dt in files_exist: 
        continue
    files_to_ingest.append((dt, filepath))
#files_to_ingest

# COMMAND ----------

if len(files_to_ingest) > 0:
    header = next(csv.reader(open(files_to_ingest[0][1], "r")))
    table_schema = StructType([StructField(column, 
                                 StringType() if column[-5:] != "_date" else DateType(), 
                                 True) for column in header]) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion

# COMMAND ----------

if len(files_to_ingest) > 0:
    df = (spark.read.format("csv")
            .option("header", "false")
            .option("skipRows", "1")
            .schema(table_schema)
            .load(str(files_to_ingest[0][1])))

# COMMAND ----------

writemode = "overwrite"
for item in tqdm(files_to_ingest):
    df = (spark.read.format("csv")
            .option("header", "false")
            .option("skipRows", "1")
            .schema(table_schema)
            .load(str(item[1])))
    
    # drop columns that do not provide any info
    # - replacement_npi => always the same as npi
    # - employer_identification_number_ein => either NULL or <UNAVAIL>
    # - npi_deactivation_reason_code => always NULL
    # - parent_organization_tin => either NULL or <UNAVAIL>

    df = df.drop("replacement_npi", 
                 "employer_identification_number_ein",
                 "npi_deactivation_reason_code",
                 "parent_organization_tin")
    
    if spark.catalog.tableExists(f"{catalog}.{schema}.{tablename}"):
        writemode = "append"
    
    # Certification Date column is added in 2020. To ingest the older files, we set "mergeSchema = True"
    (df.write
        .format("delta")
        .mode(writemode)
        .option("mergeSchema", "true")
        .saveAsTable(f"{catalog}.{schema}.{tablename}"))

# COMMAND ----------


