## Imports
from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

## DLT - Bookings

#### Staging
@dp.table(
  name="staging_bookings"
)
def stage_bookings():
    df = spark.readStream.format("delta").load("/Volumes/workspace/bronze/bronze_volume/bookings/data")
    return df

#### Transformations
@dp.temporary_view(
    name="transform_bookings"
    )
def transform_bookings():
    df = spark.readStream.table("staging_bookings")
    df = df.withColumn("amount", col("amount").cast("double")) \
           .withColumn("modified_date", current_timestamp()) \
           .withColumn("booking_date", to_date(col("booking_date"))) \
           .drop("_rescued_data")
    return df

#### Define and Apply Rules
bookings_rules = {
    "rule_1": "booking_id IS NOT NULL",
    "rule_2": "passenger_id IS NOT NULL",
    "rule_3": "flight_id IS NOT NULL"
}
@dp.table(
  name="silver_bookings"
)
@dp.expect_all_or_drop(bookings_rules)
def silver_bookings():
    df = dp.readStream("transform_bookings")
    return df