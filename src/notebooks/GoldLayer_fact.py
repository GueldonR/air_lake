# Databricks notebook source
# MAGIC %md
# MAGIC # GoldLayer_fact

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# Catalog
catalog = "workspace"

# Fact merge key
fact_merge_key = "booking_id"

#CDC Columns
cdc_col = "modified_date"

#Source Schema
source_schema = "silver"

# Target Schema
target_schema = "gold"

#Target object
target_object = "fact_table"

#Source fact table
fact_table = f"{catalog}.{source_schema}.silver_bookings"

# Fact columns to select from source (measures and keys)
fact_columns = ["booking_id", "amount", "booking_date", "modified_date"]

# COMMAND ----------

# List of dimension parameter configuration
dim_configs = [
    {
        "table": f"{catalog}.{target_schema}.dim_passengers",
        "alias": "dim_passengers", 
        "join_keys": [("passenger_id", "passenger_id")], # fact col, dim col
        "surrogate_key": "dim_passengers_key"
    },

    {
        "table": f"{catalog}.{target_schema}.dim_flights",
        "alias": "dim_flights", 
        "join_keys": [("flight_id", "flight_id")], # fact col, dim col
        "surrogate_key": "dim_flight_key"
    },

    {
        "table": f"{catalog}.{target_schema}.dim_airports",
        "alias": "dim_airports",
        "join_keys": [("airport_id", "airport_id")], # fact col, dim col
        "surrogate_key": "dim_airports_key"
    }
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dynamic fact query

# COMMAND ----------

def generate_fact_query_incremental(fact_table, dimension, fact_columns, cdc_column, processing_date):
    fact_alias = "f"

    # Column select
    select_cols = [f"{fact_alias}.{col}" for col in fact_columns]

    #Build joins

    join_clauses = []
    for dim in dim_configs:
        table_full_name = dim["table"]
        alias = dim["alias"]
        surrogate_key = f"{alias}.{dim['surrogate_key']}"
        select_cols.append(surrogate_key)

        # join clause 
        on_clause = [f"{fact_alias}.{fk} = {alias}.{dk}" for fk, dk in dim["join_keys"]]
        join_clause = f"left join {table_full_name} {alias} on " + " and ".join(on_clause)
        join_clauses.append(join_clause)

    #Select and join
    select_clause = ",\n   ".join(select_cols)
    joins = "\n  ".join(join_clauses)

    # where clause
    where_clause = f"{fact_alias}.{cdc_column} >= DATE('{processing_date}')"


    # Final query

    query = f"""
    select
        {select_clause}
    from 
        {fact_table} {fact_alias}
    {joins}
    where {where_clause}
    """.strip()

    return query

# COMMAND ----------

# MAGIC %md
# MAGIC #### Defining last load

# COMMAND ----------

# Get last load date for incremental processing
if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
    last_load = spark.sql(f"select max({cdc_col}) from {catalog}.{target_schema}.{target_object}").collect()[0][0]
else:
    last_load = "1900-01-01"  # Default starting date for initial load

print(f"Last load date: {last_load}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Generate and execute fact query

# COMMAND ----------

query = generate_fact_query_incremental(fact_table=fact_table, dimension=dim_configs, fact_columns=fact_columns, cdc_column=cdc_col, processing_date = last_load)

# COMMAND ----------

df_fact = spark.sql(query)
#df_fact.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upsert

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepping surrogate keys and 

# COMMAND ----------

# Extract surrogate keys from dimension_config
surrogate_keys_list = [dim["surrogate_key"] for dim in dim_configs]
print("Surrogate keys:", surrogate_keys_list)

# For fact table, using the booking_id for merge
print(f"Fact table merge key: {fact_merge_key}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Merge logic for fact table

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
    dlt_obj = DeltaTable.forName(spark, f"{catalog}.{target_schema}.{target_object}")
    dlt_obj.alias("trg").merge(df_fact.alias("src"), f"trg.{fact_merge_key} = src.{fact_merge_key}")\
        .whenMatchedUpdateAll(condition = f"src.{cdc_col} >= trg.{cdc_col}")\
        .whenNotMatchedInsertAll()\
        .execute()
else:
    df_fact.write.format("delta")\
        .mode("append")\
        .saveAsTable(f"{catalog}.{target_schema}.{target_object}")