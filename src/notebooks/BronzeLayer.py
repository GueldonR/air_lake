# Databricks notebook source
# MAGIC %md
# MAGIC # Incremental ingestion for bronze layer

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read CSV data with Auto Loader

# COMMAND ----------

dbutils.widgets.text("src", defaultValue="")

# COMMAND ----------

src_value = dbutils.widgets.get("src")

print(src_value)

# COMMAND ----------

df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option(
        "cloudFiles.schemaLocation",
        f"/Volumes/workspace/bronze/bronze_volume/{src_value}/checkpoint",
    )
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .load(
        f"/Volumes/workspace/raw_source_schema/raw_source_volume/raw_data/{src_value}/"
    )
)

print(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta table

# COMMAND ----------

df.writeStream.format("delta").outputMode("append").trigger(once=True).option(
    "checkpointLocation",
    f"/Volumes/workspace/bronze/bronze_volume/{src_value}/checkpoint",
).option("path", f"/Volumes/workspace/bronze/bronze_volume/{src_value}/data").start()

# COMMAND ----------

# MAGIC %md
# MAGIC
