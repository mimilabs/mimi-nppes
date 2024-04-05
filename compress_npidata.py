# Databricks notebook source
from pyspark.sql.functions import col, udf, collect_list, coalesce, lit
from pyspark.sql.types import ArrayType, StringType, IntegerType

# COMMAND ----------

def get_uniques(values, dates):
    pairs = sorted([(v, d) for v, d in zip(values, dates)], 
                   key=lambda x: x[1], 
                   reverse=True)
    val_last = pairs[0][0]
    out = [f"{pairs[0][0]}@{pairs[0][1]}"]
    for pair in pairs[1:]:
        if pair[0] != val_last:
            out.append(f"{pair[0]}@{pair[1]}")
            val_last = pair[0]
    return out

get_uniques_udf = udf(get_uniques, ArrayType(StringType()))

# COMMAND ----------

df = (spark.read.table('mimi_ws_1.nppes.npidata')
        .groupBy("npi")
        .agg(collect_list(coalesce(col("other_provider_identifiers"), lit(""))).alias("other_ids"),
            collect_list(col("_input_file_date")).alias("dates"))
        .withColumn("other_ids_unq", get_uniques_udf(col("other_ids"), col("dates")))
        )

# COMMAND ----------

df.display(10)

# COMMAND ----------


