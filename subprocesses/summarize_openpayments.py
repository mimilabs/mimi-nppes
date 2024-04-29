# Databricks notebook source
# look at `npidata` and `pl`
from pyspark.sql.functions import (col, 
                                   lit, 
                                   concat_ws, 
                                   coalesce, 
                                   collect_list, size)

# COMMAND ----------

catalog = "mimi_ws_1"
schema_in = "openpayments"
schema_out = "nppes"
table_in0 = "deleted"
table_in1 = "general"
table_in2 = "research"
table_in3 = "ownership" # Need to understand the structure more
table_out = "openpayments" # npi

# COMMAND ----------

# when I join with the record ID, I found out that all the records are "pre-removed" from the general/research payment data. So, we do not need to join this table to clean up.
df_deleted = (spark.read.table(f"{catalog}.{schema_in}.{table_in0}")
                .select("record_id", "change_type")
                .withColumnRenamed("change_type", "ctd"))

# COMMAND ----------

# ignore hospitals for now; as it needs CCN to NPI mapping
# op: open payment variables
df1 = (spark.read.table(f"{catalog}.{schema_in}.{table_in1}")
        .filter(col("covered_recipient_npi").isNotNull())
        .filter(col("total_amount_of_payment_usdollars").isNotNull())
        .filter(col("date_of_payment").isNotNull())
        .withColumn("type_of_payment", lit("general"))
        .withColumnRenamed("covered_recipient_npi", "npi")
        .select("npi",
            "type_of_payment",
            "total_amount_of_payment_usdollars",
            "date_of_payment",
            coalesce(col("applicable_manufacturer_or_applicable_gpo_making_payment_name"),
                     lit("")).alias("op1"),
            coalesce(col("form_of_payment_or_transfer_of_value"),lit("")).alias("op2"),
            coalesce(col("nature_of_payment_or_transfer_of_value"),lit("")).alias("op3"),
            coalesce(col("indicate_drug_or_biological_or_device_or_medical_supply_1"),
                     lit("")).alias("op4"),
            coalesce(col("product_category_or_therapeutic_area_1"),lit("")).alias("op5"),
            coalesce(col("name_of_drug_or_biological_or_device_or_medical_supply_1"),
                     lit("")).alias("op6")))

# COMMAND ----------

# ignore hospitals for now; as it needs CCN to NPI mapping
# op: open payment variables
df2 = (spark.read.table(f"{catalog}.{schema_in}.{table_in2}")
        .filter(col("covered_recipient_npi").isNotNull())
        .filter(col("total_amount_of_payment_usdollars").isNotNull())
        .filter(col("date_of_payment").isNotNull())
        .withColumn("type_of_payment", lit("research"))
        .withColumnRenamed("covered_recipient_npi", "npi")
        .select("npi",
                "type_of_payment",
                "total_amount_of_payment_usdollars",
                "date_of_payment",
                coalesce(col("applicable_manufacturer_or_applicable_gpo_making_payment_name"),
                     lit("")).alias("op1"),
                coalesce(col("form_of_payment_or_transfer_of_value"), lit("")).alias("op2"),
                coalesce(col("name_of_study"), lit("")).alias("op3"),
                coalesce(col("indicate_drug_or_biological_or_device_or_medical_supply_1"), 
                            lit("")).alias("op4"),
                coalesce(col("product_category_or_therapeutic_area_1"), lit("")).alias("op5"),
                coalesce(col("name_of_drug_or_biological_or_device_or_medical_supply_1"), 
                            lit("")).alias("op6")))

# COMMAND ----------

df = df1.union(df2)

# COMMAND ----------

df = (df.groupBy("npi")
        .agg(collect_list(concat_ws("^", col("type_of_payment"),
                                    col("total_amount_of_payment_usdollars"),
                                    col("date_of_payment"),
                                    col("op1"),
                                    col("op2"),
                                    col("op3"),
                                    col("op4"),
                                    col("op5"),
                                    col("op6")
                                    )
                          ).alias("openpayments")
             ))

# COMMAND ----------

(df.write
    .format('delta')
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(f"{catalog}.{schema_out}.{table_out}"))

# COMMAND ----------


