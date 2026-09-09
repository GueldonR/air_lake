# Databricks notebook source
# MAGIC %md
# MAGIC # Define source parameters

# COMMAND ----------

src_array = [
    {"src": "bookings"},
    {"src": "flights"},
    {"src": "passenger"},
    {"src": "airports"},
]

# COMMAND ----------

# MAGIC %md
# MAGIC # Set task output values

# COMMAND ----------

dbutils.jobs.taskValues.set(key="output_key", value=src_array)
