## Imports
import yaml
from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

## Bronze - raw CSV files ingested incrementally with Auto Loader

catalog = spark.conf.get("airlake.catalog")
bronze_schema = spark.conf.get("airlake.bronze_schema")
raw_volume = spark.conf.get("airlake.raw_volume").rstrip("/")

# Source tables are defined in config/sources.yml, one raw folder and one bronze table each
with open(spark.conf.get("airlake.sources_config")) as f:
    SOURCES = yaml.safe_load(f)["sources"]


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


for source in SOURCES:
    create_bronze_table(source)
