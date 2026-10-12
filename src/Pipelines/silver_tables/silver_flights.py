## Imports
from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

## DLT - Flights


#### Transform
@dp.temporary_view(name="transform_flights")
def transform_flights():
    df = spark.readStream.table(
        f"{spark.conf.get('airlake.catalog')}.{spark.conf.get('airlake.bronze_schema')}.flights"
    )
    df = df.drop("_rescued_data", "_ingested_at", "_source_file")
    df = df.withColumn("modified_date", current_timestamp())
    return df


# Stream changes
dp.create_streaming_table(name="silver_flights")

dp.create_auto_cdc_flow(
    name="silver_flights",
    source="transform_flights",
    target="silver_flights",
    keys=["flight_id"],
    sequence_by=col("modified_date"),
    stored_as_scd_type=1,
)
