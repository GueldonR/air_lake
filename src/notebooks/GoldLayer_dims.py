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

# Dimension configuration
dimensions = {
    "airports": {
        "source_object": "silver_airports",
        "target_object": "dim_airports",
        "key_columns": ["airport_id"],
        "surrogate_key": "dim_airports_key",
    },
    "flights": {
        "source_object": "silver_flights",
        "target_object": "dim_flights",
        "key_columns": ["flight_id"],
        "surrogate_key": "dim_flight_key",
    },
    "passengers": {
        "source_object": "silver_passengers",
        "target_object": "dim_passengers",
        "key_columns": ["passenger_id"],
        "surrogate_key": "dim_passengers_key",
    },
}

dbutils.widgets.dropdown("dimension", "airports", list(dimensions.keys()))
dbutils.widgets.text("backdated_refresh", "")

dimension = dbutils.widgets.get("dimension")
backdated_refresh = dbutils.widgets.get("backdated_refresh")

if dimension not in dimensions:
    raise ValueError(
        f"Unsupported dimension '{dimension}'. "
        f"Choose one of: {list(dimensions.keys())}"
    )

config = dimensions[dimension]
key_col_list = config["key_columns"]
source_object = config["source_object"]
target_object = config["target_object"]
surrogate_key = config["surrogate_key"]

cdc_col = "modified_date"
source_schema = "silver"
target_schema = "gold"
catalog = "workspace"

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
