# Databricks notebook source
# MAGIC %md
# MAGIC # Load sample data into raw folders
# MAGIC Copies `data/sample/<dataset>/` onto the raw volume. `dataset`: `initial`, `incremental` or `scd`.

# COMMAND ----------

dbutils.widgets.text("dataset", "initial")
dbutils.widgets.text("raw_volume", "")
dbutils.widgets.text("sample_path", "")

src = f"file:{dbutils.widgets.get('sample_path')}/{dbutils.widgets.get('dataset')}"
dbutils.fs.cp(src, dbutils.widgets.get("raw_volume"), recurse=True)
