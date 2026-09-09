# Databricks notebook source
# MAGIC %md
# MAGIC **Create raw source volume**

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("bronze_schema", "bronze")
dbutils.widgets.text("silver_schema", "silver")
dbutils.widgets.text("gold_schema", "gold")
dbutils.widgets.text("raw_source_schema", "raw_source_schema")
dbutils.widgets.text("raw_source_volume", "raw_source_volume")
dbutils.widgets.text(
    "raw_volume",
    "/Volumes/workspace/raw_source_schema/raw_source_volume/raw_data",
)

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
gold_schema = dbutils.widgets.get("gold_schema")
raw_source_schema = dbutils.widgets.get("raw_source_schema")
raw_source_volume = dbutils.widgets.get("raw_source_volume")
raw_volume = dbutils.widgets.get("raw_volume").rstrip("/")

spark.sql(
    f"CREATE VOLUME IF NOT EXISTS {catalog}.{raw_source_schema}.{raw_source_volume}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Create raw data folder**

# COMMAND ----------

dbutils.fs.mkdirs(raw_volume)

# COMMAND ----------

# MAGIC %md
# MAGIC **Create dimension folders**

# COMMAND ----------

dimensions = {
    "airports",
    "bookings",
    "flights",
    "passenger",
}

for d in dimensions:
    existing_directories = {item.name.rstrip("/") for item in dbutils.fs.ls(raw_volume)}
    if d not in existing_directories:
        dbutils.fs.mkdirs(f"{raw_volume}/{d}")

    else:
        print(f"Directory {d} already exists")

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Create layer schemas**

# COMMAND ----------

for schema in (gold_schema, silver_schema, bronze_schema):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Create volume for each layer**

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{bronze_schema}.bronze_volume")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{silver_schema}.silver_volume")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{gold_schema}.gold_volume")
