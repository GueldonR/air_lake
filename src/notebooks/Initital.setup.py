# Databricks notebook source
# MAGIC %md
# MAGIC **Create raw source volume**

# COMMAND ----------

# MAGIC %sql
# MAGIC Create volume if not exists
# MAGIC     workspace.raw_source_schema.raw_source_volume

# COMMAND ----------

# MAGIC %md
# MAGIC **Create raw data folder**

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/workspace/raw_source_schema/raw_source_volume/raw_data")

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
    existing_directories = {
        item.name.rstrip("/")
        for item in dbutils.fs.ls(
            "/Volumes/workspace/raw_source_schema/raw_source_volume/raw_data"
        )
    }
    if d not in existing_directories:
        dbutils.fs.mkdirs(
            f"/Volumes/workspace/raw_source_schema/raw_source_volume/raw_data/{d}"
        )

    else:
        print(f"Directory {d} already exists")

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Create layer schemas**

# COMMAND ----------

# MAGIC %sql
# MAGIC Create schema workspace.gold;
# MAGIC Create schema workspace.silver;
# MAGIC Create schema workspace.bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC **Create volume for each layer**

# COMMAND ----------

# MAGIC %sql
# MAGIC Create volume if not exists
# MAGIC     workspace.bronze.bronze_volume;
# MAGIC
# MAGIC Create volume if not exists
# MAGIC     workspace.silver.silver_volume;
# MAGIC
# MAGIC Create volume if not exists
# MAGIC     workspace.gold.gold_volume;
