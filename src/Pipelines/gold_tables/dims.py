## Imports
from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

## Gold dimensions
#  Surrogate key: xxhash64 of the natural key, so it stays the same on every refresh.
#  (Identity columns "might be recomputed during updates to a materialized view", Databricks docs.)
#  Unknown row: every dimension has a row with key -1, and facts point to it when a lookup finds nothing.

gold = f"{spark.conf.get('airlake.catalog')}.{spark.conf.get('airlake.gold_schema')}"


@dp.materialized_view(name=f"{gold}.dim_date", comment="One row per day, from the first to the last date in the data")
def dim_date():
    schedules = spark.read.table("flight_schedules")
    all_dates = (
        schedules.select(F.col("valid_from").alias("day"))
        .union(schedules.select("valid_to"))
        .union(spark.read.table("bookings").select(F.to_date("booked_at")))
        .union(spark.read.table("payments").select(F.to_date("created_at")))
    )
    first_and_last = all_dates.agg(F.min("day").alias("first_day"), F.max("day").alias("last_day"))
    calendar = first_and_last.select(F.explode(F.sequence("first_day", "last_day")).alias("date"))

    dates = calendar.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),  # e.g. 20260315
        "date",
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        (F.weekday("date") + 1).alias("day_of_week"),  # 1 = Monday
        F.date_format("date", "EEEE").alias("day_name"),
        (F.weekday("date") >= 5).alias("is_weekend"),
    )
    unknown = spark.sql("select -1 as date_key, 'Unknown' as month_name, 'Unknown' as day_name")
    return dates.unionByName(unknown, allowMissingColumns=True)


@dp.materialized_view(name=f"{gold}.dim_airline", comment="SCD1: one row per airline")
def dim_airline():
    airlines = spark.read.table("airlines").select(
        F.xxhash64("airline_id").alias("airline_sk"),
        "airline_id",
        "iata_code",
        "airline_name",
        "country",
    )
    unknown = spark.sql("select -1 as airline_sk, 'UNKNOWN' as airline_id, 'Unknown' as airline_name")
    return airlines.unionByName(unknown, allowMissingColumns=True)


@dp.materialized_view(name=f"{gold}.dim_airport", comment="SCD1: one row per airport, used as both origin and destination")
def dim_airport():
    airports = spark.read.table("airports").select(
        F.xxhash64("airport_id").alias("airport_sk"),
        "airport_id",
        "iata_code",
        "airport_name",
        "city",
        "country",
        "latitude",
        "longitude",
    )
    unknown = spark.sql("select -1 as airport_sk, 'UNKNOWN' as airport_id, 'Unknown' as airport_name")
    return airports.unionByName(unknown, allowMissingColumns=True)


@dp.materialized_view(name=f"{gold}.dim_passenger", comment="SCD1: one row per passenger, without direct identifiers")
def dim_passenger():
    # Names, email, phone and full birth date stay in silver (GDPR data minimisation)
    passengers = spark.read.table("passengers").select(
        F.xxhash64("passenger_id").alias("passenger_sk"),
        "passenger_id",
        "nationality",
        F.year("date_of_birth").alias("birth_year"),
    )
    unknown = spark.sql("select -1 as passenger_sk, 'UNKNOWN' as passenger_id, 'Unknown' as nationality")
    return passengers.unionByName(unknown, allowMissingColumns=True)


@dp.materialized_view(name=f"{gold}.dim_aircraft", comment="SCD2: one row per aircraft version")
def dim_aircraft():
    first_version = Window.partitionBy("aircraft_id").orderBy("__START_AT")
    versions = spark.read.table("aircraft").select(
        "*",
        # The first version starts in 1900, so flights from before silver saw the aircraft still find it
        F.when(F.row_number().over(first_version) == 1, F.lit("1900-01-01").cast("timestamp"))
        .otherwise(F.col("__START_AT"))
        .alias("valid_from"),
        F.col("__END_AT").alias("valid_to"),
    )
    aircraft = versions.select(
        F.xxhash64("aircraft_id", "valid_from").alias("aircraft_sk"),
        "aircraft_id",
        "registration",
        "model",
        "seat_capacity",
        "status",
        "valid_from",
        "valid_to",
        F.col("valid_to").isNull().alias("is_current"),
    )
    unknown = spark.sql("select -1 as aircraft_sk, 'UNKNOWN' as aircraft_id, 'Unknown' as model")
    return aircraft.unionByName(unknown, allowMissingColumns=True)
