# Databricks notebook source
df = spark.read.table("mimi_ws_1.nppes.othername")

# COMMAND ----------

summaryDf = (df.groupBy("provider_other_organization_name_type_code", "_input_file_name")
                    .count())


# COMMAND ----------

summaryDf.display()

# COMMAND ----------

summaryDf = (df.groupBy("npi", "_input_file_name")
                    .count())

# COMMAND ----------

summaryDf.display()

# COMMAND ----------

df.filter(df.npi == '1639716921').display()

# COMMAND ----------


