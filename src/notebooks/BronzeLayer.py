# Databricks notebook source
# MAGIC %md
# MAGIC # Incremental ingestion for bronze layer

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read CSV data with Auto Loader

# COMMAND ----------

dbutils.widgets.text("src", defaultValue="")
dbutils.widgets.text(
    "raw_volume",
    defaultValue="/Volumes/workspace/raw_source_schema/raw_source_volume/raw_data",
)
dbutils.widgets.text(
    "bronze_volume",
    defaultValue="/Volumes/workspace/bronze/bronze_volume",
)

# COMMAND ----------

src_value = dbutils.widgets.get("src")
raw_volume = dbutils.widgets.get("raw_volume").rstrip("/")
bronze_volume = dbutils.widgets.get("bronze_volume").rstrip("/")

print(src_value)

# COMMAND ----------

df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option(
        "cloudFiles.schemaLocation",
        f"{bronze_volume}/{src_value}/checkpoint",
    )
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .load(f"{raw_volume}/{src_value}/")
)

print(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta table

# COMMAND ----------

df.writeStream.format("delta").outputMode("append").trigger(once=True).option(
    "checkpointLocation",
    f"{bronze_volume}/{src_value}/checkpoint",
).option("path", f"{bronze_volume}/{src_value}/data").start()

# COMMAND ----------

# MAGIC %md
# MAGIC
