## Imports
from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

#### Staging


@dp.temporary_view(name="transform_airports")
def transform_airports():
    df = spark.readStream.format("delta").load(
        f"{spark.conf.get('airlake.bronze_volume', '/Volumes/workspace/bronze/bronze_volume').rstrip('/')}/airports/data"
    )
    df = df.drop("_rescued_data").withColumn("modified_date", current_timestamp())
    return df


dp.create_streaming_table(name="silver_airports")

dp.create_auto_cdc_flow(
    target="silver_airports",
    source="transform_airports",
    keys=["airport_id"],
    stored_as_scd_type=1,
    sequence_by=col("modified_date"),
)
