## Imports
import yaml
from pyspark import pipelines as dp
from pyspark.sql import functions as F

## Silver - entity tables: transform view -> expectations -> quarantine table -> AUTO CDC

catalog = spark.conf.get("airlake.catalog")
bronze_schema = spark.conf.get("airlake.bronze_schema")

# Source names, keys and silver settings live in config/sources.yml, shared with bronze
with open(spark.conf.get("airlake.sources_config")) as f:
    SOURCES = yaml.safe_load(f)["sources"]


def build_silver(table, scd_type, casts, rules, cleanups=None):
    """Create <table>_validated, <table>_quarantine, <table>_valid and the silver <table>.

    scd_type 1 keeps the current version per key; 2 keeps every version with __START_AT/__END_AT.
    cleanups run before casts; casts use try_cast so bad values become NULL and fail a rule.
    """
    key = SOURCES[table]["key"]

    #### Transform: clean, cast, and record which rules each row breaks
    @dp.temporary_view(name=f"{table}_validated")
    @dp.expect_all(rules)
    def validated():
        df = spark.readStream.table(f"{catalog}.{bronze_schema}.{table}")
        for column, expression in (cleanups or {}).items():
            df = df.withColumn(column, F.expr(expression))
        for column, sql_type in casts.items():
            df = df.withColumn(column, F.expr(f"try_cast({column} AS {sql_type})"))
        # Rule name if the row breaks the rule, else NULL (a NULL result falls through to ELSE)
        failed = [
            F.expr(f"CASE WHEN {rule} THEN NULL ELSE '{name}' END")
            for name, rule in rules.items()
        ]
        return df.withColumn("_failed_rules", F.array_compact(F.array(*failed)))

    #### Quarantine: rows that broke at least one rule, with the rule names and source file
    @dp.table(
        name=f"{table}_quarantine",
        comment=f"{table} rows that failed silver expectations",
    )
    def quarantine():
        return spark.readStream.table(f"{table}_validated").filter(
            F.size("_failed_rules") > 0
        )

    #### Load: valid rows upserted by key, ordered by updated_at
    @dp.temporary_view(name=f"{table}_valid")
    def valid():
        return spark.readStream.table(f"{table}_validated").filter(
            F.size("_failed_rules") == 0
        )

    dp.create_streaming_table(
        name=table,
        comment=(
            f"{table}: current state per key"
            if scd_type == 1
            else f"{table}: full version history (SCD2)"
        ),
    )
    dp.create_auto_cdc_flow(
        target=table,
        source=f"{table}_valid",
        keys=[key],
        sequence_by=F.col("updated_at"),
        stored_as_scd_type=scd_type,
        except_column_list=["_failed_rules", "_rescued_data", "_source_file"],
    )


## Tables
# Per-table settings (scd_type, cleanups, casts, rules) live in config/sources.yml;
# every source with a silver block gets built here.
for table, source in SOURCES.items():
    if "silver" in source:
        build_silver(table, **source["silver"])
