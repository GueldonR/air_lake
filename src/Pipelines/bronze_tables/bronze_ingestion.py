## Imports
from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

## Bronze - raw CSV files ingested incrementally with Auto Loader

catalog = spark.conf.get("airlake.catalog")
bronze_schema = spark.conf.get("airlake.bronze_schema")
raw_volume = spark.conf.get("airlake.raw_volume").rstrip("/")

# One bronze table per raw folder
sources = ["airports", "bookings", "flights", "passenger"]


def create_bronze_table(source):
    @dp.table(
        name=f"{catalog}.{bronze_schema}.{source}",
        comment=f"Raw {source} CSV files, appended as they arrive",
    )
    def bronze_table():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            .load(f"{raw_volume}/{source}/")
            .withColumn("_ingested_at", current_timestamp())
            .withColumn("_source_file", col("_metadata.file_path"))
        )


for source in sources:
    create_bronze_table(source)
