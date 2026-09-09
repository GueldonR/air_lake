# Silver Buisness View
## Imports
from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.materialized_view(
    name = "silver_buisness_mat_view",
)
def silver_buisness_mat_view():
    df = spark.read.table("silver_bookings")\
        .join(spark.read.table("silver_flights"), ["flight_id"])\
        .join(spark.read.table("silver_passengers"), ["passenger_id"])\
        .join(spark.read.table("silver_airports"), ["airport_id"])\
        .drop("modified_date")
    return df