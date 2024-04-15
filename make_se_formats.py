# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # SE (Start-End) Formatted Tables
# MAGIC
# MAGIC This script creates various SE-format data, where SE stands for Start and End. 
# MAGIC Many data entries change over time in NPPES, and we want to summarize the lifetime of each data value using Start and End dates. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configurations

# COMMAND ----------

from pyspark.sql.functions import (col, lit, udf, collect_list, coalesce,
                                   explode, split, to_date, concat_ws)
from pyspark.sql.types import ArrayType, StringType, IntegerType
import pandas as pd
from dateutil.relativedelta import relativedelta
catalog = "mimi_ws_1"
schema = "nppes"

idtype_map = {
    "01": "OTHER", 
    "02": "MEDICARE UPIN", 
    "04": "MEDICARE ID-TYPE UNSPECIFIED", 
    "05": "MEDICAID", 
    "06": "MEDICARE OSCAR/CERTIFICATION",
    "07": "MEDICARE NSC", 
    "08": "MEDICARE PIN"}
exceptions = {
    "1699392548": {
        "1255972063^01^^NPPES | INDIVIDUAL NPI": "1255972063^01^^NPPES  INDIVIDUAL NPI"},
    "1265511885": {
        "1032600^01^MA^|MA SOCIAL WORK LICENSE": "1032600^01^MA^MA SOCIAL WORK LICENSE"},
    "1033196803": {
        "000000379333^^01^MI^ANTHEM|4788534^05^MI^": "000000379333^01^MI^ANTHEM|4788534^05^MI^",
        "4788534^05^MI^|000000379333^^01^MI^ANTHEM": "4788534^05^MI^|000000379333^01^MI^ANTHEM"
    }}
taxonomy_map = {}
taxonomy_filepath = "/Volumes/mimi_ws_1/nppes/src/taxonomy/nucc_taxonomy_240.csv"
for _, row in pd.read_csv(taxonomy_filepath).iterrows():
    taxonomy_map[row["Code"]] = row["Display Name"]
othername_map = {
    "1": "Former Name",
    "2": "Professional Name",
    "3": "Doing Business As",
    "4": "Former Legal Business Name",
    "5": "Other Name"
}

def get_se_lst(npi, 
               values, 
               dates, 
               category="otherid"):
    
    # get the last observed list
    tmp = {}

    # Some old NPPES records allowed special characters such as "|" and "^"
    # We remove those here to make the data consistent
    values_ = values
    if category == "otherid" and npi in exceptions:
        values_ = [exceptions[npi].get(x, x) for x in values]

    for v, d in zip(values_, dates):
        for token in v.split("|"):
            # edit the token accordingly
            token_ = token
            subtokens = token_.split("^")
            if category == "otherid": 
                subtoken = subtokens[1]
                subtoken_desc = idtype_map.get(subtoken, "")
                token_ = f"{token}^{subtoken_desc}"
            elif category == "taxonomy":
                token_ = (f"{subtokens[0]}^{subtokens[-1]}"
                        f"^{taxonomy_map.get(subtokens[0],'')}")
            elif category == "license":
                token_ = f"{subtokens[1]}^{subtokens[2]}"
            elif category == "othername":
                token_ = f"{token}^{othername_map.get(subtokens[1],'')}"
            elif category == "endpoint" or category == "pl": 
                pass # do nothing
            
            # collect the timestamp for each token's observation
            if token_ not in tmp:
                tmp[token_] = [d]
            else:
                tmp[token_].append(d)
    
    # collect the consecutive timestamps for each token
    # so that we know the continuous start and end dates.
    out = []
    for token, date_lst in tmp.items():
        date_lst_ = sorted(date_lst)
        s = date_lst_[0]
        pairs = []
        for d1, d2 in zip(date_lst_[:-1], date_lst_[1:]):
            # more than one month apart, then treat it disconnected
            if (d2 - d1).days > 45: 
                out.append(f"{token}^{s}^{d1}")
                s = d2
        out.append(f"{token}^{s}^{date_lst_[-1]}")

    return out

get_se_lst_udf = udf(get_se_lst, ArrayType(StringType()))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Other Provider IDs

# COMMAND ----------

table = "otherid_se" #se: start-end format
df = (spark.read.table('mimi_ws_1.nppes.npidata')
        .filter(col("other_provider_identifiers").isNotNull())
        .groupBy("npi")
        .agg(collect_list(col("other_provider_identifiers")).alias("other_ids"),
            collect_list(col("_input_file_date")).alias("dates"))
        .withColumn("other_id_token", 
                    explode(get_se_lst_udf(col("npi"), 
                                           col("other_ids"), 
                                           col("dates"), 
                                           lit("otherid"))))
        .select("npi", "other_id_token")
        .withColumn("other_id", split(col("other_id_token"), "\^").getItem(0))
        .withColumn("other_id_type_code", split(col("other_id_token"), "\^").getItem(1))
        .withColumn("other_id_state", split(col("other_id_token"), "\^").getItem(2))
        .withColumn("other_id_issuer", split(col("other_id_token"), "\^").getItem(3))
        .withColumn("other_id_type_desc", split(col("other_id_token"), "\^").getItem(4))
        .withColumn("other_id_dt_s", to_date(split(col("other_id_token"), "\^").getItem(5)))
        .withColumn("other_id_dt_e", to_date(split(col("other_id_token"), "\^").getItem(6)))
        .filter(col("other_id") != "")
        )

# COMMAND ----------

(df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{catalog}.{schema}.{table}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Taxonomies

# COMMAND ----------

table = "taxonomy_se" #se: start-end format
df = (spark.read.table('mimi_ws_1.nppes.npidata')
        .filter(col("healthcare_provider_taxonomies").isNotNull())
        .groupBy("npi")
        .agg(collect_list(col("healthcare_provider_taxonomies")).alias("taxonomies"),
            collect_list(col("_input_file_date")).alias("dates"))
        .withColumn("taxonomy_token", 
                    explode(get_se_lst_udf(col("npi"), 
                                           col("taxonomies"), 
                                           col("dates"),
                                           lit("taxonomy"))))
        .select("npi", "taxonomy_token")
        .withColumn("taxonomy_code", split(col("taxonomy_token"), "\^").getItem(0))
        .withColumn("is_main", split(col("taxonomy_token"), "\^").getItem(1))
        .withColumn("taxonomy_desc", split(col("taxonomy_token"), "\^").getItem(2))
        .withColumn("taxonomy_dt_s", to_date(split(col("taxonomy_token"), "\^").getItem(3)))
        .withColumn("taxonomy_dt_e", to_date(split(col("taxonomy_token"), "\^").getItem(4)))
        .filter(col("taxonomy_code") != "")
        )

# COMMAND ----------

(df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{catalog}.{schema}.{table}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Licenses

# COMMAND ----------

table = "license_se" #se: start-end format
df = (spark.read.table('mimi_ws_1.nppes.npidata')
        .filter(col("healthcare_provider_taxonomies").isNotNull())
        .groupBy("npi")
        .agg(collect_list(col("healthcare_provider_taxonomies")).alias("taxonomies"),
            collect_list(col("_input_file_date")).alias("dates"))
        .withColumn("license_token", 
                    explode(get_se_lst_udf(col("npi"), 
                                           col("taxonomies"), 
                                           col("dates"),
                                           lit("license"))))
        .select("npi", "license_token")
        .withColumn("license_number", split(col("license_token"), "\^").getItem(0))
        .withColumn("license_state", split(col("license_token"), "\^").getItem(1))
        .withColumn("license_dt_s", to_date(split(col("license_token"), "\^").getItem(2)))
        .withColumn("license_dt_e", to_date(split(col("license_token"), "\^").getItem(3)))
        .filter(col("license_number") != "")
        )

# COMMAND ----------

(df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{catalog}.{schema}.{table}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Othername, Pl, and Endpoint

# COMMAND ----------

# MAGIC %md
# MAGIC ### Othername

# COMMAND ----------

table = "othername_se" #se: start-end format
df = (spark.read.table('mimi_ws_1.nppes.othername')
        .withColumn("othername_key", 
                        concat_ws("^", 
                            coalesce(col("provider_other_organization_name"), lit("")), 
                            coalesce(col("provider_other_organization_name_type_code"), lit(""))))
        .groupBy("npi")
        .agg(collect_list(col("othername_key")).alias("othername_keys"),
            collect_list(col("_input_file_date")).alias("dates"))
        .withColumn("othername_token", 
                    explode(get_se_lst_udf(col("npi"), 
                                           col("othername_keys"), 
                                           col("dates"),
                                           lit("othername"))))
        .select("npi", "othername_token")
        .withColumn("othername", split(col("othername_token"), "\^").getItem(0))
        .withColumn("othername_type_code", split(col("othername_token"), "\^").getItem(1))
        .withColumn("othername_type_desc", split(col("othername_token"), "\^").getItem(2))
        .withColumn("othername_dt_s", to_date(split(col("othername_token"), "\^").getItem(3)))
        .withColumn("othername_dt_e", to_date(split(col("othername_token"), "\^").getItem(4)))
        .filter(col("othername") != "")
        )

# COMMAND ----------

(df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{catalog}.{schema}.{table}"))

# COMMAND ----------

table = "othername_se" #se: start-end format
# individual other names are stored in the original npidata table
df = (spark.read.table('mimi_ws_1.nppes.npidata')
        .select("npi", 
                "provider_other_last_name",
                "provider_other_first_name", 
                "provider_other_middle_name",
                "provider_other_last_name_type_code",
                "_input_file_date")
        .withColumn("othername_key", 
                        concat_ws("^", 
                            concat_ws(" ", 
                                      col("provider_other_first_name"),
                                      col("provider_other_middle_name"),
                                      col("provider_other_last_name")), 
                            coalesce(col("provider_other_last_name_type_code"), lit(""))))
        .groupBy("npi")
        .agg(collect_list(col("othername_key")).alias("othername_keys"),
            collect_list(col("_input_file_date")).alias("dates"))
        .withColumn("othername_token", 
                    explode(get_se_lst_udf(col("npi"), 
                                           col("othername_keys"), 
                                           col("dates"),
                                           lit("othername"))))
        .select("npi", "othername_token")
        .withColumn("othername", split(col("othername_token"), "\^").getItem(0))
        .withColumn("othername_type_code", split(col("othername_token"), "\^").getItem(1))
        .withColumn("othername_type_desc", split(col("othername_token"), "\^").getItem(2))
        .withColumn("othername_dt_s", to_date(split(col("othername_token"), "\^").getItem(3)))
        .withColumn("othername_dt_e", to_date(split(col("othername_token"), "\^").getItem(4)))
        .filter(col("othername") != "")
        )

# COMMAND ----------

(df.write
    .format("delta")
    .mode("append")
    .saveAsTable(f"{catalog}.{schema}.{table}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Endpoint

# COMMAND ----------

table = "endpoint_se" #se: start-end format
df = (spark.read.table('mimi_ws_1.nppes.endpoint')
        .withColumn("endpoint_key", concat_ws("^", 
                                            coalesce(col("endpoint_type"), lit("")), 
                                            coalesce(col("endpoint"), lit(""))))
        .groupBy("npi")
        .agg(collect_list(col("endpoint_key")).alias("endpoint_keys"),
            collect_list(col("_input_file_date")).alias("dates"))
        .withColumn("endpoint_token", 
                    explode(get_se_lst_udf(col("npi"), 
                                           col("endpoint_keys"), 
                                           col("dates"),
                                           lit("endpoint"))))
        .select("npi", "endpoint_token")
        .withColumn("endpoint_type", split(col("endpoint_token"), "\^").getItem(0))
        .withColumn("endpoint", split(col("endpoint_token"), "\^").getItem(1))
        .withColumn("endpoint_dt_s", to_date(split(col("endpoint_token"), "\^").getItem(2)))
        .withColumn("endpoint_dt_e", to_date(split(col("endpoint_token"), "\^").getItem(3)))
        .filter(col("endpoint") != "")
        )

# COMMAND ----------

(df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{catalog}.{schema}.{table}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pl

# COMMAND ----------

table = "pl_se" #se: start-end format
df = (spark.read.table('mimi_ws_1.nppes.pl')
        .withColumn("pl_key", 
                    concat_ws("^", 
                        coalesce(col("provider_secondary_practice_location_address_address_line_1"), lit("")), 
                        coalesce(col("provider_secondary_practice_location_address_address_line_1"), lit("")),
                        coalesce(col("provider_secondary_practice_location_address__city_name"), lit("")),
                        coalesce(col("provider_secondary_practice_location_address__state_name"), lit("")),
                        coalesce(col("provider_secondary_practice_location_address__postal_code"), lit(""))))
        .groupBy("npi")
        .agg(collect_list(col("pl_key")).alias("pl_keys"),
            collect_list(col("_input_file_date")).alias("dates"))
        .withColumn("pl_token", 
                    explode(get_se_lst_udf(col("npi"), 
                                           col("pl_keys"), 
                                           col("dates"),
                                           lit("pl"))))
        .select("npi", "pl_token")
        .withColumn("pl_dt_s", to_date(split(col("pl_token"), "\^").getItem(5)))
        .withColumn("pl_dt_e", to_date(split(col("pl_token"), "\^").getItem(6)))
        )

# COMMAND ----------

(df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{catalog}.{schema}.{table}"))

# COMMAND ----------


