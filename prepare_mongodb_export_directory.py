# Databricks notebook source
!pip install pymongo

# COMMAND ----------

from pyspark.sql.functions import (col, lit, max as _max, size, slice,
                                   collect_list, collect_set, 
                                   concat_ws, concat, first, any_value, when, count)
import pymongo
catalog = "mimi_ws_1"
schema = "nppes"
database = "nppes" #mongodb
collection = "directory" #mongodb
mdb_password = dbutils.secrets.get(scope="mdb", key="MDB_PASSWORD")
uri = f"mongodb+srv://databricks:{mdb_password}@mimi2.urvlr.mongodb.net/{database}"

# COMMAND ----------

reval_max_ifd = (spark.read.table("mimi_ws_1.datacmsgov.reval")
                .select(_max(col("_input_file_date")).alias("max_ifd"))
                .collect()[0]["max_ifd"])

# COMMAND ----------

df_ppef = (spark.read.table("mimi_ws_1.datacmsgov.pc_provider")
               .select(col("npi").alias("group_npi"), 
                       col("pecos_asct_cntl_id").alias("group_pac_id"))
               .dropDuplicates(["group_pac_id"]))

# COMMAND ----------

df_reval_base = (spark.read.table("mimi_ws_1.datacmsgov.reval")
    .filter(col("_input_file_date")==reval_max_ifd)
    .join(df_ppef, on=["group_pac_id"], how="left")
    .withColumn("individual_name_npi", concat_ws("^",
                                        concat_ws(" ", col("individual_first_name"),
                                                col("individual_last_name")),
                                        col("individual_npi")))
    .withColumn("group_name_npi", concat_ws("^", 
                                            col("group_legal_business_name"),
                                            col("group_npi"))))
df = (df_reval_base
        .select(col("group_npi").alias("npi"), "individual_name_npi", "group_name_npi")
        .dropDuplicates()
        .groupBy("npi")
        .agg(collect_set(col("individual_name_npi")).alias("employees"),
            first(col("group_name_npi")).alias("group_name_npi"))
        .withColumn("employee_cnt", size(col("employees")))
        )

# COMMAND ----------

df = (df.withColumn("category", (when(col("employee_cnt") > 5000, "5000+")
                            .when(col("employee_cnt") > 1000, "1000+")
                            .when(col("employee_cnt") > 500, "500+")
                            .when(col("employee_cnt") > 100, "100+")
                            .when(col("employee_cnt") > 50, "50+")
                            .when(col("employee_cnt") > 10, "10+")
                            .when(col("employee_cnt") > 5, "5+")
                            .when(col("employee_cnt") > 1, "1+")
                            .otherwise("sole")))
        .withColumn("initial", 
                              col("group_name_npi").substr(1,1)))

# COMMAND ----------

(df.write.format("com.mongodb.spark.sql.DefaultSource")
                .option("spark.mongodb.output.uri", uri)
                .option("database", database)
                .option("collection", collection)
                .mode("overwrite")
                .save())

# COMMAND ----------

client = pymongo.MongoClient(uri)
conn = client[database][collection]
conn.create_index([("category", pymongo.ASCENDING), ("initial", pymongo.ASCENDING)])
conn.create_index([("npi", pymongo.ASCENDING)])
