# Databricks notebook source
!pip install h3

# COMMAND ----------

# look at `npidata` and `pl`
from pyspark.sql.functions import (col, 
                                   countDistinct, 
                                   lit, 
                                   udf,
                                   concat_ws, 
                                   coalesce, 
                                   substring, 
                                   desc,
                                   max as _max,
                                   regexp_replace, collect_list, size)
from pyspark.sql.types import StringType
import h3

# COMMAND ----------

catalog = "mimi_ws_1"
schema = "nppes"
table_in = "npidata"
table_out = "address_h3" # npi, name, address_key_b, address_key_m, h3_b, h3_m, npi_lst_b, npi_lst_m
h3_res = 12

# COMMAND ----------

def get_h3(lat, lng):
    if lat is None or lng is None:
        return ""
    else:
        return h3.geo_to_h3(lat, lng, h3_res)
get_h3_udf = udf(get_h3, StringType())

# COMMAND ----------

df_h3 = (spark.read.table(f"{catalog}.{schema}.address_census")
                    .filter(col("census_status")=="Match")
                    .withColumn("h3", 
                                get_h3_udf(col("census_latitude"), 
                                           col("census_longitude")))
                    .select("address_key", "h3"))

# COMMAND ----------

def clean(x):
    return coalesce(regexp_replace(x, "|", ""), lit(""))

# COMMAND ----------

npidata_max_ifd = (spark.read.table(f"{catalog}.{schema}.{table_in}")
                .select(_max(col("_input_file_date")).alias("max_ifd"))
                .collect()[0]["max_ifd"])

# COMMAND ----------

df = (spark.read.table(f"{catalog}.{schema}.{table_in}")
        .filter(col("_input_file_date")==npidata_max_ifd)
        .filter(col("entity_type_code").isNotNull())
        .select("npi",
            "entity_type_code",
            "provider_organization_name_legal_business_name",
            "provider_last_name_legal_name",
            "provider_first_name",
            col("provider_first_line_business_practice_location_address").alias("line1_b"),
            col("provider_second_line_business_practice_location_address").alias("line2_b"),
            col("provider_business_practice_location_address_city_name").alias("city_b"),
            col("provider_business_practice_location_address_state_name").alias("state_b"),
            col("provider_business_practice_location_address_postal_code").alias("zipcode_b"),
            col("provider_business_practice_location_address_country_code_if_outside_us").alias("country_b"),
            col("provider_first_line_business_mailing_address").alias("line1_m"),
            col("provider_second_line_business_mailing_address").alias("line2_m"),
            col("provider_business_mailing_address_city_name").alias("city_m"),
            col("provider_business_mailing_address_state_name").alias("state_m"),
            col("provider_business_mailing_address_postal_code").alias("zipcode_m"),
            col("provider_business_mailing_address_country_code_if_outside_us").alias("country_m"))
        .withColumn("address_key_b", concat_ws("|", 
                                            clean(col("line1_b")), 
                                            clean(col("line2_b")), 
                                            clean(col("city_b")),  
                                            clean(col("state_b")), 
                                            substring(clean(col("zipcode_b")), 0, 5), 
                                            clean(col("country_b"))))
        .withColumn("address_key_m", concat_ws("|", 
                                            clean(col("line1_m")), 
                                            clean(col("line2_m")), 
                                            clean(col("city_m")),  
                                            clean(col("state_m")), 
                                            substring(clean(col("zipcode_m")), 0, 5), 
                                            clean(col("country_m"))))
        .withColumn("name", concat_ws(" ", col("provider_first_name"),
                                        col("provider_last_name_legal_name"),
                                        col("provider_organization_name_legal_business_name")))
        .select("npi", "entity_type_code", "name", "address_key_b", "address_key_m")
)

# COMMAND ----------

df = (df.join(df_h3, on=df.address_key_b==df_h3.address_key, how="left")
        .withColumnRenamed("h3", "h3_b")
        .drop("address_key")
        .join(df_h3, on=df.address_key_m==df_h3.address_key, how="left")
        .withColumnRenamed("h3", "h3_m")
        .drop("address_key")
        )

# COMMAND ----------

df_b = (df.filter(col("entity_type_code")=="2")
            .groupBy("h3_b")
            .agg(collect_list(concat_ws("^", col("npi"), 
                                            col("entity_type_code"), 
                                            col("name"))).alias("npi_lst_b")))

# COMMAND ----------

df_m = (df.filter(col("entity_type_code")=="2")
            .groupBy("h3_m")
            .agg(collect_list(concat_ws("^", col("npi"), 
                                            col("entity_type_code"), 
                                            col("name"))).alias("npi_lst_m")))

# COMMAND ----------

df = (df.join(df_b, on=["h3_b"], how="left")
        .join(df_m, on=["h3_m"], how="left")
        .drop("address_key_b", "address_key_m"))

# COMMAND ----------

(df.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.{table_out}"))
