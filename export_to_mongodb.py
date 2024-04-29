# Databricks notebook source
# MAGIC %md
# MAGIC ## Export to Mongo
# MAGIC
# MAGIC Use at least the HighPerf1 cluster, or others with the spark-mongodb connector installed
# MAGIC
# MAGIC https://docs.databricks.com/en/connect/external-systems/mongodb.html
# MAGIC
# MAGIC Need to use the exact JAR version. For this project, we downloaded the JAR in one of our Volume storages. 

# COMMAND ----------

!pip install pymongo

# COMMAND ----------

import pymongo


# COMMAND ----------

catalog = "mimi_ws_1"
schema = "nppes"
table = "mongodb_export"
database = "nppes" #mongodb
collection = "npidata" #mongodb
mdb_password = dbutils.secrets.get(scope="mdb", key="MDB_PASSWORD")
uri = f"mongodb+srv://databricks:{mdb_password}@mimi2.urvlr.mongodb.net/{database}"
client = pymongo.MongoClient(uri)


# COMMAND ----------

conn = client[database][collection]
conn.drop()

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

conn = client[database][collection]

# COMMAND ----------

conn.create_index([("npi", pymongo.ASCENDING)])

# COMMAND ----------

"""
conn.create_search_index({
    "definition": {
        "mappings": {
            "dynamic": True,
            "fields": {
                "address_search": {
                    "type": "string"
                },
                "name_search": {
                    "type": "string"
                },
                "npi": {
                    "type": "string"
                }
            }
        }
    },
    "name": "npi_name_address"
})
"""

# COMMAND ----------

conn.create_search_index({
    "definition": {
        "mappings": {
            "dynamic": True,
            "fields": {
                "address_search": {
                    "type": "string"
                },
                "name_search": {
                    "type": "string"
                },
                "npi": {
                    "type": "string"
                }, 
                "reval_specialty": {
                    "type": "string"
                }
            }
        }
    },
    "name": "npi_name_address_specialty"
})

# COMMAND ----------


