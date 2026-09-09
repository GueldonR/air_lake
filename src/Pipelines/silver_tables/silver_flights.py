## Imports
from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

## DLT - Flights


#### Transform
@dp.temporary_view(name="transform_flights")
def transform_flights():
    df = spark.readStream.format("delta").load(
        f"{spark.conf.get('airlake.bronze_volume', '/Volumes/workspace/bronze/bronze_volume').rstrip('/')}/flights/data"
    )
    df = df.drop("_rescued_data").withColumn("modified_date", current_timestamp())
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
