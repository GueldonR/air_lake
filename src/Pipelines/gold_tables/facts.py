## Imports
from pyspark import pipelines as dp
from pyspark.sql import functions as F

## Gold facts
#  Each fact swaps natural keys for surrogate keys with a left join to the dimension.
#  fillna(-1) then points rows without a match at the dimension's Unknown row instead of dropping them.
#  Expectations only warn: the pipeline UI counts those Unknown rows.

gold = f"{spark.conf.get('airlake.catalog')}.{spark.conf.get('airlake.gold_schema')}"


@dp.materialized_view(name=f"{gold}.fact_flight", comment="Accumulating snapshot: one row per flight, updated as it departs and arrives")
@dp.expect_all({
    "airline_known": "airline_sk <> -1",
    "route_known": "origin_airport_sk <> -1 AND destination_airport_sk <> -1",
    "aircraft_known": "aircraft_sk <> -1",
})
def fact_flight():
    flights = spark.read.table("flights").filter("__END_AT IS NULL")  # current version of each flight
    schedules = spark.read.table("flight_schedules").select(
        "schedule_id", "airline_id", "origin_airport_id", "destination_airport_id"
    )
    airlines = spark.read.table(f"{gold}.dim_airline").select("airline_id", "airline_sk")
    origins = spark.read.table(f"{gold}.dim_airport").select(
        F.col("airport_id").alias("origin_airport_id"),
        F.col("airport_sk").alias("origin_airport_sk"),
    )
    destinations = spark.read.table(f"{gold}.dim_airport").select(
        F.col("airport_id").alias("destination_airport_id"),
        F.col("airport_sk").alias("destination_airport_sk"),
    )

    # SCD2: the aircraft version that was valid at departure
    aircraft = spark.read.table(f"{gold}.dim_aircraft").select(
        F.col("aircraft_id").alias("version_aircraft_id"), "aircraft_sk", "valid_from", "valid_to"
    )
    valid_at_departure = (
        (F.col("aircraft_id") == F.col("version_aircraft_id"))
        & (F.col("scheduled_departure") >= F.col("valid_from"))
        & (F.col("valid_to").isNull() | (F.col("scheduled_departure") < F.col("valid_to")))
    )

    return (
        flights
        .join(schedules, "schedule_id", "left")
        .join(airlines, "airline_id", "left")
        .join(origins, "origin_airport_id", "left")
        .join(destinations, "destination_airport_id", "left")
        .join(aircraft, valid_at_departure, "left")
        .select(
            "flight_id",
            "flight_number",
            "status",
            F.date_format("flight_date", "yyyyMMdd").cast("int").alias("flight_date_key"),
            "airline_sk",
            "origin_airport_sk",
            "destination_airport_sk",
            "aircraft_sk",
            "scheduled_departure",
            "actual_departure",
            "scheduled_arrival",
            "actual_arrival",
            F.expr("timestampdiff(MINUTE, scheduled_departure, actual_departure)").alias("departure_delay_min"),
            F.expr("timestampdiff(MINUTE, scheduled_arrival, actual_arrival)").alias("arrival_delay_min"),
            (F.col("status") == "cancelled").cast("int").alias("is_cancelled"),
        )
        .fillna(-1, ["airline_sk", "origin_airport_sk", "destination_airport_sk", "aircraft_sk"])
    )


@dp.materialized_view(name=f"{gold}.fact_booking_segment", comment="Transaction: one row per passenger per flight")
@dp.expect_all({
    "passenger_known": "passenger_sk <> -1",
    "flight_known": "flight_date_key <> -1",
})
def fact_booking_segment():
    segments = spark.read.table("booking_segments")
    bookings = spark.read.table("bookings").filter("__END_AT IS NULL").select(
        "booking_id",
        "booking_reference",
        "passenger_id",
        "booked_at",
        "channel",
        "currency",
        F.col("status").alias("booking_status"),
    )
    passengers = spark.read.table(f"{gold}.dim_passenger").select("passenger_id", "passenger_sk")
    # Flight keys come from fact_flight, so both facts agree on date, route and aircraft
    flights = spark.read.table(f"{gold}.fact_flight").select(
        "flight_id",
        "flight_number",
        "flight_date_key",
        "airline_sk",
        "origin_airport_sk",
        "destination_airport_sk",
        "aircraft_sk",
    )

    return (
        segments
        .join(bookings, "booking_id", "left")
        .join(passengers, "passenger_id", "left")
        .join(flights, "flight_id", "left")
        .select(
            "segment_id",
            "segment_no",
            "booking_reference",
            "flight_id",
            "flight_number",
            "cabin_class",
            "channel",
            "booking_status",
            "currency",
            F.date_format("booked_at", "yyyyMMdd").cast("int").alias("booking_date_key"),
            "flight_date_key",
            "passenger_sk",
            "airline_sk",
            "origin_airport_sk",
            "destination_airport_sk",
            "aircraft_sk",
            "fare_amount",
        )
        .fillna(-1, [
            "booking_date_key", "flight_date_key", "passenger_sk",
            "airline_sk", "origin_airport_sk", "destination_airport_sk", "aircraft_sk",
        ])
    )


@dp.materialized_view(name=f"{gold}.fact_payment", comment="Transaction: one row per payment or refund")
@dp.expect("passenger_known", "passenger_sk <> -1")
def fact_payment():
    payments = spark.read.table("payments")
    bookings = spark.read.table("bookings").filter("__END_AT IS NULL").select(
        "booking_id", "booking_reference", "passenger_id"
    )
    passengers = spark.read.table(f"{gold}.dim_passenger").select("passenger_id", "passenger_sk")

    # Refunds are negative. Filter out status = 'failed' to count only money that moved.
    signed_amount = F.when(F.col("payment_type") == "refund", -F.col("amount")).otherwise(F.col("amount"))

    return (
        payments
        .join(bookings, "booking_id", "left")
        .join(passengers, "passenger_id", "left")
        .select(
            "payment_id",
            "booking_reference",
            "payment_type",
            "method",
            "status",
            "currency",
            F.date_format("created_at", "yyyyMMdd").cast("int").alias("payment_date_key"),
            "passenger_sk",
            signed_amount.alias("amount"),
        )
        .fillna(-1, ["passenger_sk"])
    )


@dp.materialized_view(name=f"{gold}.fact_aircraft_daily", comment="Periodic snapshot: one row per aircraft per day, as at end of day")
def fact_aircraft_daily():
    days = spark.read.table(f"{gold}.dim_date").filter("date_key <> -1").select(
        "date_key", F.date_add("date", 1).cast("timestamp").alias("end_of_day")
    )
    aircraft = spark.read.table(f"{gold}.dim_aircraft").filter("aircraft_sk <> -1")

    # The aircraft version that was valid at the end of each day
    valid_at_end_of_day = (F.col("valid_from") <= F.col("end_of_day")) & (
        F.col("valid_to").isNull() | (F.col("end_of_day") < F.col("valid_to"))
    )
    is_active = (F.col("status") == "active").cast("int")

    return days.join(aircraft, valid_at_end_of_day).select(
        "date_key",
        "aircraft_sk",
        "seat_capacity",
        is_active.alias("is_active"),
        (F.col("seat_capacity") * is_active).alias("active_seats"),  # sum across aircraft, never across days
    )


@dp.materialized_view(name=f"{gold}.fact_schedule_day", comment="Factless coverage: one row per scheduled departure per day")
@dp.expect_all({
    "airline_known": "airline_sk <> -1",
    "route_known": "origin_airport_sk <> -1 AND destination_airport_sk <> -1",
})
def fact_schedule_day():
    # Scheduled but never flown: fact_schedule_day LEFT ANTI JOIN fact_flight USING (flight_number, flight_date_key)
    schedules = spark.read.table("flight_schedules")
    airlines = spark.read.table(f"{gold}.dim_airline").select("airline_id", "airline_sk")
    origins = spark.read.table(f"{gold}.dim_airport").select(
        F.col("airport_id").alias("origin_airport_id"),
        F.col("airport_sk").alias("origin_airport_sk"),
    )
    destinations = spark.read.table(f"{gold}.dim_airport").select(
        F.col("airport_id").alias("destination_airport_id"),
        F.col("airport_sk").alias("destination_airport_sk"),
    )

    # One row per day the schedule is valid, kept on the weekdays it flies ('1357' = Mon, Wed, Fri, Sun)
    schedule_days = (
        schedules
        .withColumn("flight_date", F.explode(F.sequence("valid_from", "valid_to")))
        .filter(F.col("days_of_week").contains((F.weekday("flight_date") + 1).cast("string")))
    )

    return (
        schedule_days
        .join(airlines, "airline_id", "left")
        .join(origins, "origin_airport_id", "left")
        .join(destinations, "destination_airport_id", "left")
        .select(
            "flight_number",
            F.date_format("flight_date", "yyyyMMdd").cast("int").alias("flight_date_key"),
            "airline_sk",
            "origin_airport_sk",
            "destination_airport_sk",
        )
        .fillna(-1, ["airline_sk", "origin_airport_sk", "destination_airport_sk"])
    )


@dp.materialized_view(name=f"{gold}.agg_route_month", comment="Aggregate: seats sold and revenue per route and month")
def agg_route_month():
    return (
        spark.read.table(f"{gold}.fact_booking_segment")
        .filter("booking_status IS DISTINCT FROM 'cancelled'")  # cancelled bookings are not revenue
        .groupBy(
            F.floor(F.col("flight_date_key") / 100).alias("month_key"),  # 20260315 -> 202603 (Unknown -1 stays -1)
            "airline_sk",
            "origin_airport_sk",
            "destination_airport_sk",
            "currency",
        )
        .agg(
            F.count("*").alias("seats_sold"),
            F.sum("fare_amount").alias("revenue"),
        )
    )
