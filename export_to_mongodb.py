# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import (col, lit, max as _max, 
                                   collect_list, collect_set, 
                                   concat_ws, first)

catalog = "mimi_ws_1"
schema = "nppes"
table = "mongodb_export"
database = "nppes" #mongodb
collection = "npidata" #mongodb
mdb_password = dbutils.secrets.get(scope="mdb", key="MDB_PASSWORD")
uri = f"mongodb+srv://databricks:{mdb_password}@mimi2.urvlr.mongodb.net/{database}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summarize/Join tables

# COMMAND ----------

df_base = (spark.read.table("mimi_ws_1.nppes.npidata")
        .filter(col("entity_type_code").isNotNull())
        .groupBy("npi")
        .agg(_max(col("_input_file_date")).alias("_input_file_date")))
df_npidata = (spark.read.table("mimi_ws_1.nppes.npidata")
                .select("npi", 
                        "entity_type_code", 
                        "provider_organization_name_legal_business_name",
                        "provider_last_name_legal_name", 
                        "provider_first_name",
                        "provider_middle_name",
                        "provider_name_prefix_text", 
                        "provider_name_suffix_text", 
                        "provider_credential_text",
                        "provider_first_line_business_practice_location_address",
                        "provider_second_line_business_practice_location_address",
                        "provider_business_practice_location_address_city_name",
                        "provider_business_practice_location_address_state_name",
                        "provider_business_practice_location_address_postal_code",
                        "provider_business_practice_location_address_country_code_if_outside_us",
                        "provider_business_practice_location_address_telephone_number",
                        "provider_business_practice_location_address_fax_number",
                        "provider_enumeration_date",
                        "npi_reactivation_date",
                        "provider_gender_code",
                        "parent_organization_lbn",
                        "certification_date",
                        "_input_file_date"))

# COMMAND ----------

reval_max_ifd = (spark.read.table("mimi_ws_1.datacmsgov.reval")
                .select(_max(col("_input_file_date")).alias("max_ifd"))
                .collect()[0]["max_ifd"])
optout_max_ifd = (spark.read.table("mimi_ws_1.datacmsgov.optout")
                .select(_max(col("_input_file_date")).alias("max_ifd"))
                .collect()[0]["max_ifd"])

# COMMAND ----------

df_optout = (spark.read.table("mimi_ws_1.datacmsgov.optout")
             .filter(col("_input_file_date")==optout_max_ifd)
             .select("npi", "optout_effective_date", "optout_end_date")
             .groupBy("npi")
             .agg(_max(col("optout_effective_date")).alias("optout_effective_date"),
                  _max(col("optout_end_date")).alias("optout_end_date")))
df_reval = (spark.read.table("mimi_ws_1.datacmsgov.reval")
    .filter(col("_input_file_date")==reval_max_ifd)
    .select("group_pac_id", 
            "group_legal_business_name",
            "individual_specialty_description",
            "individual_due_date",
            "individual_pac_id",
            col("individual_npi").alias("npi"))
    .dropDuplicates()
    .groupBy("npi")
    .agg(_max(col("individual_due_date")).alias("reval_due_date"),
         first(col("individual_specialty_description")).alias("reval_specialty"),
         collect_set(col("group_legal_business_name")).alias("groups"),
         first(col("individual_pac_id")).alias("pac_id"))
    )

# COMMAND ----------

df_pl = (spark.read.table("mimi_ws_1.nppes.pl_se")
            .groupBy("npi")
            .agg(collect_list("pl_token").alias("places")))
df_tx = (spark.read.table("mimi_ws_1.nppes.taxonomy_se")
            .groupBy("npi")
            .agg(collect_list("taxonomy_token").alias("taxonomies")))
df_on = (spark.read.table("mimi_ws_1.nppes.othername_se")
            .groupBy("npi")
            .agg(collect_list("othername_token").alias("othernames")))
df_lc = (spark.read.table("mimi_ws_1.nppes.license_se")
            .groupBy("npi")
            .agg(collect_list("license_token").alias("licenses")))
df_ep = (spark.read.table("mimi_ws_1.nppes.endpoint_se")
            .groupBy("npi")
            .agg(collect_list("endpoint_token").alias("endpoints")))

# COMMAND ----------

df = (df_base.join(df_npidata, on=["npi", "_input_file_date"], how="left")
        .join(df_reval, on=["npi"], how="left")
        .join(df_optout, on=["npi"], how="left")
        .join(df_pl, on=["npi"], how="left")
        .join(df_tx, on=["npi"], how="left")
        .join(df_on, on=["npi"], how="left")
        .join(df_lc, on=["npi"], how="left")
        .join(df_ep, on=["npi"], how="left"))

# COMMAND ----------

(df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{catalog}.{schema}.{table}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export to Mongo
# MAGIC
# MAGIC Use at least the HighPerf1 cluster, or others with the spark-mongodb connector installed
# MAGIC
# MAGIC https://docs.databricks.com/en/connect/external-systems/mongodb.html
# MAGIC
# MAGIC Need to use the exact JAR version. For this project, we downloaded the JAR in one of our Volume storages. 

# COMMAND ----------

df = spark.read.table(f"{catalog}.{schema}.{table}")
# sometimes, "mongodb" can be replaced with "com.mongodb.spark.sql.DefaultSource"
(df.write.format("com.mongodb.spark.sql.DefaultSource")
                .option("spark.mongodb.output.uri", uri)
                .option("database", database)
                .option("collection", collection)
                .mode("overwrite")
                .save())

# COMMAND ----------

!pip install pymongo

# COMMAND ----------

import pymongo
client = pymongo.MongoClient(uri)
conn = client[database][collection]

# COMMAND ----------

conn.create_index([("npi", pymongo.ASCENDING)], unique=True)

# COMMAND ----------


