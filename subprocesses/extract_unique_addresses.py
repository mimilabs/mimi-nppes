# Databricks notebook source
# look at `npidata` and `pl`
from pyspark.sql.functions import (col, 
                                   countDistinct, 
                                   lit, 
                                   concat_ws, 
                                   coalesce, 
                                   substring, 
                                   regexp_replace)

# COMMAND ----------

catalog = "mimi_ws_1"
schema = "nppes"
table_in1 = "npidata"
table_in2 = "pl"
table_out = "address_key"

# COMMAND ----------

def clean(x):
    return coalesce(regexp_replace(x, "|", ""), lit(""))

# COMMAND ----------

df_b1 = (spark.read.table(f"{catalog}.{schema}.{table_in1}")
        .select("npi",
            col("provider_first_line_business_practice_location_address").alias("line1"),
            col("provider_second_line_business_practice_location_address").alias("line2"),
            col("provider_business_practice_location_address_city_name").alias("city"),
            col("provider_business_practice_location_address_state_name").alias("state"),
            col("provider_business_practice_location_address_postal_code").alias("zipcode"),
            col("provider_business_practice_location_address_country_code_if_outside_us").alias("country"))
        .withColumn("address_key", concat_ws("|", 
                                            clean(col("line1")), 
                                            clean(col("line2")), 
                                            clean(col("city")),  
                                            clean(col("state")), 
                                            substring(clean(col("zipcode")), 0, 5), 
                                            clean(col("country"))))
        .groupBy("address_key")
        .agg(countDistinct(col("npi")).alias("npi_b1_cnt"))
        .select("address_key", "npi_b1_cnt")
        )

# COMMAND ----------

df_m1 = (spark.read.table(f"{catalog}.{schema}.{table_in1}")
        .select("npi",
            col("provider_first_line_business_mailing_address").alias("line1"),
            col("provider_second_line_business_mailing_address").alias("line2"),
            col("provider_business_mailing_address_city_name").alias("city"),
            col("provider_business_mailing_address_state_name").alias("state"),
            col("provider_business_mailing_address_postal_code").alias("zipcode"),
            col("provider_business_mailing_address_country_code_if_outside_us").alias("country"))
        .withColumn("address_key", concat_ws("|", 
                                            clean(col("line1")), 
                                            clean(col("line2")), 
                                            clean(col("city")),  
                                            clean(col("state")), 
                                            substring(clean(col("zipcode")), 0, 5), 
                                            clean(col("country"))))
        .groupBy("address_key")
        .agg(countDistinct(col("npi")).alias("npi_m1_cnt"))
        .select("address_key", "npi_m1_cnt")
        )

# COMMAND ----------

table = "pl"
df_b2 = (spark.read.table(f"{catalog}.{schema}.{table_in2}")
        .select("npi",
            col("provider_secondary_practice_location_address_address_line_1").alias("line1"),
            col("provider_secondary_practice_location_address__address_line_2").alias("line2"),
            col("provider_secondary_practice_location_address__city_name").alias("city"),
            col("provider_secondary_practice_location_address__state_name").alias("state"),
            col("provider_secondary_practice_location_address__postal_code").alias("zipcode"),
            col("provider_secondary_practice_location_address__country_code_if_outside_us").alias("country"))
        .withColumn("address_key", concat_ws("|", 
                                            clean(col("line1")), 
                                            clean(col("line2")), 
                                            clean(col("city")),  
                                            clean(col("state")), 
                                            substring(clean(col("zipcode")), 0, 5), 
                                            clean(col("country"))))
        .groupBy("address_key")
        .agg(countDistinct(col("npi")).alias("npi_b2_cnt"))
        .select("address_key", "npi_b2_cnt")
        )

# COMMAND ----------

df_merged = (df_b1.join(df_b2, 
                       on=["address_key"],
                       how="outer")
              .join(df_m1, 
                        on=["address_key"], 
                        how="outer"))

# COMMAND ----------

(df_merged.write.format("delta")
        .mode("overwrite")
        .saveAsTable(f"{catalog}.{schema}.{table_out}"))

# COMMAND ----------


