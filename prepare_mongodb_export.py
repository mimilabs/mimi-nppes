# Databricks notebook source
from pyspark.sql.functions import (col, lit, max as _max, size, slice,
                                   collect_list, collect_set, 
                                   concat_ws, concat, first, any_value)

catalog = "mimi_ws_1"
schema = "nppes"
table = "mongodb_export"

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
dac_max_ifd = (spark.read.table("mimi_ws_1.provdatacatalog.dac_ndf")
                .select(_max(col("_input_file_date")).alias("max_ifd"))
                .collect()[0]["max_ifd"])

# COMMAND ----------

df_optout = (spark.read.table("mimi_ws_1.datacmsgov.optout")
             .filter(col("_input_file_date")==optout_max_ifd)
             .select("npi", "optout_effective_date", "optout_end_date")
             .groupBy("npi")
             .agg(_max(col("optout_effective_date")).alias("optout_effective_date"),
                  _max(col("optout_end_date")).alias("optout_end_date")))
df_ppef = (spark.read.table("mimi_ws_1.datacmsgov.pc_provider")
               .select(col("npi").alias("group_npi"), 
                       col("pecos_asct_cntl_id").alias("group_pac_id"))
               .dropDuplicates(["group_pac_id"]))
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

max_show = 40 # employers with above "max_show" needs to contact the data lakehouse admin
df_reval_employee = (df_reval_base
                     .select(col("group_npi").alias("npi"), "individual_name_npi")
                     .dropDuplicates()
                     .groupBy("npi")
                     .agg(collect_set(col("individual_name_npi")).alias("employees_"))
                     .withColumn("employee_cnt", size(col("employees_")))
                     .withColumn("employees", slice(col("employees_"), 1, max_show))
                     .drop("employees_")
                     )
df_reval = (df_reval_base.select("group_pac_id", 
            "group_name_npi",
            "individual_specialty_description",
            "individual_due_date",
            "individual_pac_id",
            col("individual_npi").alias("npi"))
    .dropDuplicates()
    .groupBy("npi")
    .agg(_max(col("individual_due_date")).alias("reval_due_date"),
         first(col("individual_specialty_description")).alias("reval_specialty"),
         collect_set(col("group_name_npi")).alias("groups"),
         first(col("individual_pac_id")).alias("pac_id"))
    )

# COMMAND ----------

df_ndf = (spark.read.table("mimi_ws_1.provdatacatalog.dac_ndf")
             .filter(col("_input_file_date")==dac_max_ifd)
             .select("npi", "med_sch", "grd_yr", "telephone_number")
             .groupBy("npi")
             .agg(any_value(col("med_sch"), ignoreNulls=True).alias("med_sch"),
                  any_value(col("grd_yr"), ignoreNulls=True).alias("grd_yr"),
                  any_value(col("telephone_number"), ignoreNulls=True).alias("telephone_number")))

# COMMAND ----------

df_aka = (spark.read.table("mimi_ws_1.nppes.othername_se")
    .groupBy("npi")
    .agg(concat(lit("("), 
                concat_ws(", ", collect_set(col("othername"))),
                lit(")")).alias("aka")))

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
df_oi = (spark.read.table("mimi_ws_1.nppes.otherid_se")
            .groupBy("npi")
            .agg(collect_list("other_id_token").alias("otherids")))
df_oi_ccn = (spark.read.table("mimi_ws_1.nppes.otherid_ccn_se")
            .groupBy("npi")
            .agg(collect_list("other_id_token").alias("otherids")))

# COMMAND ----------

df_h3 = (spark.read.table("mimi_ws_1.nppes.address_h3")
            .select("npi", "npi_lst_b", "npi_lst_m"))
df_openpayments = spark.read.table("mimi_ws_1.nppes.openpayments")

# COMMAND ----------

df = (df_base.join(df_npidata, on=["npi", "_input_file_date"], how="left")
        .join(df_reval, on=["npi"], how="left")
        .join(df_reval_employee, on=["npi"], how="left")
        .join(df_optout, on=["npi"], how="left")
        .join(df_pl, on=["npi"], how="left")
        .join(df_tx, on=["npi"], how="left")
        .join(df_on, on=["npi"], how="left")
        .join(df_lc, on=["npi"], how="left")
        .join(df_ep, on=["npi"], how="left")
        .join(df_oi, on=["npi"], how="left")
        .join(df_oi_ccn, on=["npi"], how="left")
        .join(df_ndf, on=["npi"], how="left")
        .join(df_aka, on=["npi"], how="left")
        .join(df_h3, on=["npi"], how="left")
        .join(df_openpayments, on=["npi"], how="left")
        )

# COMMAND ----------

df = (df.withColumn("name_search", 
                            concat_ws(" ", 
                                col("provider_first_name"),
                                col("provider_last_name_legal_name"),
                                col("provider_organization_name_legal_business_name"), 
                                col("aka")))
        .withColumn("address_search",
                    concat_ws(", ", 
                        col("provider_first_line_business_practice_location_address"),
                        col("provider_business_practice_location_address_city_name"),
                        col("provider_business_practice_location_address_state_name")))
        )

# COMMAND ----------

(df.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.{table}"))

# COMMAND ----------


