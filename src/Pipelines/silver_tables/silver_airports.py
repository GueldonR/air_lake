## Imports
from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

#### Staging


@dp.temporary_view(name="transform_airports")
def transform_airports():
    df = spark.readStream.table(
        f"{spark.conf.get('airlake.catalog')}.{spark.conf.get('airlake.bronze_schema')}.airports"
    )
    df = df.drop("_rescued_data", "_ingested_at", "_source_file")
    df = df.withColumn("modified_date", current_timestamp())
    return df


dp.create_streaming_table(name="silver_airports")

dp.create_auto_cdc_flow(
    target="silver_airports",
    source="transform_airports",
    keys=["airport_id"],
    stored_as_scd_type=1,
    sequence_by=col("modified_date"),
)
