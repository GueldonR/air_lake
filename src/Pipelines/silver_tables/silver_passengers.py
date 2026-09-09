## Imports
from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

#### Define and Apply Rules - Passengers need id
passengers_rules = {
    "rule_1": "passenger_id IS NOT NULL",
}


@dp.temporary_view(name="transform_passengers")
@dp.expect_all_or_drop(passengers_rules)
def transform_passengers():
    bronze_volume = spark.conf.get(
        "airlake.bronze_volume", "/Volumes/workspace/bronze/bronze_volume"
    ).rstrip("/")
    df = spark.readStream.format("delta").load(f"{bronze_volume}/passenger/data")
    df = df.drop("_rescued_data").withColumn("modified_date", current_timestamp())
    return df


#### Transformations
dp.create_streaming_table("silver_passengers")

dp.create_auto_cdc_flow(
    target="silver_passengers",
    source="transform_passengers",
    keys=["passenger_id"],
    stored_as_scd_type=1,
    sequence_by=col("modified_date"),
)
