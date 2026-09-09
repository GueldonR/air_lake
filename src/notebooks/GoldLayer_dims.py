# Databricks notebook source
# MAGIC %md
# MAGIC #GoldLayer_dims

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parmeters

# COMMAND ----------

# Default configuration builds the airport dimension. Run this notebook once per
# dimension, changing the values in this configuration block for each source.
key_col_list = ["airport_id"]

# CDC Columns
cdc_col = "modified_date"

# Back-dated refresh
backdated_refresh = ""

# Source object
source_object = "silver_airports"

# Source Schema
source_schema = "silver"

# Target Schema
target_schema = "gold"

# Target object
target_object = "dim_airports"

# Surrogate Key
surrogate_key = "dim_airports_key"

# Catalog
catalog = "workspace"

# COMMAND ----------

# MAGIC %skip
# MAGIC # Key col list
# MAGIC key_col  = "['flight_id']"
# MAGIC key_col_list = eval(key_col)
# MAGIC
# MAGIC #CDC Columns
# MAGIC cdc_col = "modified_date"
# MAGIC
# MAGIC #Back-dated refresh
# MAGIC backdated_refresh = ""
# MAGIC
# MAGIC #Source object
# MAGIC source_object = "silver_flights"
# MAGIC
# MAGIC #Source Schema
# MAGIC source_schema = "silver"
# MAGIC
# MAGIC # Target Schema
# MAGIC target_schema = "gold"
# MAGIC
# MAGIC #Target object
# MAGIC target_object = "dim_flights"
# MAGIC
# MAGIC #Surrogate Key
# MAGIC surrogate_key = "dim_flight_key"
# MAGIC
# MAGIC # Catalog
# MAGIC catalog = "workspace"

# COMMAND ----------

# MAGIC %skip
# MAGIC # Key col list
# MAGIC key_col  = "['passenger_id']"
# MAGIC key_col_list = eval(key_col)
# MAGIC
# MAGIC #CDC Columns
# MAGIC cdc_col = "modified_date"
# MAGIC
# MAGIC #Back-dated refresh
# MAGIC backdated_refresh = ""
# MAGIC
# MAGIC #Source object
# MAGIC source_object = "silver_passengers"
# MAGIC
# MAGIC #Source Schema
# MAGIC source_schema = "silver"
# MAGIC
# MAGIC # Target Schema
# MAGIC target_schema = "gold"
# MAGIC
# MAGIC #Target object
# MAGIC target_object = "dim_passengers"
# MAGIC
# MAGIC #Surrogate Key
# MAGIC surrogate_key = "dim_passengers_key"
# MAGIC
# MAGIC # Catalog
# MAGIC catalog = "workspace"

# COMMAND ----------

# MAGIC %skip
# MAGIC # Key col list
# MAGIC key_col  = "['airport_id']"
# MAGIC key_col_list = eval(key_col)
# MAGIC
# MAGIC #CDC Columns
# MAGIC cdc_col = "modified_date"
# MAGIC
# MAGIC #Back-dated refresh
# MAGIC backdated_refresh = ""
# MAGIC
# MAGIC #Source object
# MAGIC source_object = "silver_airports"
# MAGIC
# MAGIC #Source Schema
# MAGIC source_schema = "silver"
# MAGIC
# MAGIC # Target Schema
# MAGIC target_schema = "gold"
# MAGIC
# MAGIC #Target object
# MAGIC target_object = "dim_airports"
# MAGIC
# MAGIC #Surrogate Key
# MAGIC surrogate_key = "dim_airports_key"
# MAGIC
# MAGIC # Catalog
# MAGIC catalog = "workspace"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incremental Data Ingestion

# COMMAND ----------

# MAGIC %md
# MAGIC #### Last load date

# COMMAND ----------

# no back dated refresh
if len(backdated_refresh) == 0:
    # if table exists in the destination
    if spark.catalog.tableExists(f"workspace.{target_schema}.{target_object}"):
        last_load = spark.sql(
            f"select max({cdc_col}) from workspace.{target_schema}.{target_object}"
        ).collect()[0][0]
    else:
        last_load = "1900-01-01"

# yes backdated refresh
else:
    last_load = backdated_refresh

print(last_load)

# COMMAND ----------

df_src = spark.sql(
    f"select * from {source_schema}.{source_object} where {cdc_col} >= '{last_load}'"
)
# df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Handling old vs new records

# COMMAND ----------

if spark.catalog.tableExists(f"workspace.{target_schema}.{target_object}"):

    # Key col string
    key_col_string_incremental = ", ".join(key_col_list)

    # Key column string for incremental
    df_target = spark.sql(
        f"select {key_col_string_incremental}, {surrogate_key}, create_date, update_date from {catalog}.{target_schema}.{target_object}"
    )
else:
    # List comprehension for key columns into string
    key_col_list_init = [f"'' as {i}" for i in key_col_list]
    key_col_string_init = ", ".join(key_col_list_init)
    key_col_string_init

    # Key column string for inital
    df_target = spark.sql(f"""select {key_col_string_init},
                           cast('0' as int) as {surrogate_key},
                           cast('1900-01-01' as timestamp) as create_date,
                           cast('1900-01-01' as timestamp) as update_date""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Joining source and target

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join condition

# COMMAND ----------

join_condition = " and ".join([f"src.{i} = trg.{i}" for i in key_col_list])

# COMMAND ----------

df_src.createOrReplaceTempView("src")
df_target.createOrReplaceTempView("trg")

df_join = spark.sql(f"""
            select src.*,
            trg.{surrogate_key},
            trg.create_date,
            trg.update_date
            from src
            left join trg
            on {join_condition}
          """)

# COMMAND ----------

# old records
df_old = df_join.filter(col(f"{surrogate_key}").isNotNull())
# new records
df_new = df_join.filter(col(f"{surrogate_key}").isNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enriching DFs

# COMMAND ----------

# MAGIC %md
# MAGIC ### df_old update date

# COMMAND ----------

df_old_enriched = df_old.withColumn("update_date", current_timestamp())


# COMMAND ----------

# MAGIC %md
# MAGIC ### Preparing df_new

# COMMAND ----------

if spark.catalog.tableExists(f"workspace.{target_schema}.{target_object}"):
    max_surrogate_key = spark.sql(
        f"select max({surrogate_key}) from {catalog}.{target_schema}.{target_object}"
    ).collect()[0][0]

    df_new_enriched = (
        df_new.withColumn(
            f"{surrogate_key}",
            lit(max_surrogate_key) + lit(1) + monotonically_increasing_id(),
        )
        .withColumn("create_date", current_timestamp())
        .withColumn("update_date", current_timestamp())
    )
else:
    # if inital load
    max_surrogate_key = 0
    df_new_enriched = (
        df_new.withColumn(
            f"{surrogate_key}",
            lit(max_surrogate_key) + lit(1) + monotonically_increasing_id(),
        )
        .withColumn("create_date", current_timestamp())
        .withColumn("update_date", current_timestamp())
    )

# COMMAND ----------

max_surrogate_key

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unioning old and new records

# COMMAND ----------

df_union = df_old_enriched.unionByName(df_new_enriched)
df_union.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Upsert

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
    dlt_obj = DeltaTable.forName(spark, f"{catalog}.{target_schema}.{target_object}")
    dlt_obj.alias("trg").merge(
        df_union.alias("src"), f"trg.{surrogate_key} = src.{surrogate_key}"
    ).whenMatchedUpdateAll(
        condition=f"src.{cdc_col} >= trg.{cdc_col}"
    ).whenNotMatchedInsertAll().execute()
else:
    df_union.write.format("delta").mode("append").saveAsTable(
        f"{catalog}.{target_schema}.{target_object}"
    )

# COMMAND ----------

print(spark.sql(f"select * from {source_schema}.{source_object} limit 10"))
