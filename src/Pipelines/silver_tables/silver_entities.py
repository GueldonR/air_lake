## Imports
import yaml
from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

## Silver - validate every source, quarantine bad rows, then load
#  Entities (rows change): streaming view -> quarantine table -> AUTO CDC
#  Events (rows never change): batch view -> quarantine and table as materialized views

catalog = spark.conf.get("airlake.catalog")
bronze_schema = spark.conf.get("airlake.bronze_schema")

# Source names, keys and silver settings live in config/sources.yml, shared with bronze
with open(spark.conf.get("airlake.sources_config")) as f:
    SOURCES = yaml.safe_load(f)["sources"]

DROP_COLUMNS = ["_failed_rules", "_rescued_data", "_source_file"]


def validate(df, casts, rules, cleanups=None):
    """Clean, cast (bad values become NULL and fail a rule), and list the rules each row breaks."""
    for column, expression in (cleanups or {}).items():
        df = df.withColumn(column, F.expr(expression))
    for column, sql_type in casts.items():
        df = df.withColumn(column, F.expr(f"try_cast({column} AS {sql_type})"))
    # Rule name if the row breaks the rule, else NULL (a NULL result falls through to ELSE)
    failed = [F.expr(f"CASE WHEN {rule} THEN NULL ELSE '{name}' END") for name, rule in rules.items()]
    return df.withColumn("_failed_rules", F.array_compact(F.array(*failed)))


def build_entity(table, key, scd_type, casts, rules, cleanups=None):
    """Rows that change: SCD1 keeps the current version per key, SCD2 every version."""
    #### Transform: clean, cast, and record which rules each row breaks
    @dp.temporary_view(name=f"{table}_validated")
    @dp.expect_all(rules)
    def validated():
        bronze = spark.readStream.table(f"{catalog}.{bronze_schema}.{table}")
        return validate(bronze, casts, rules, cleanups)

    #### Quarantine: rows that broke at least one rule, with the rule names and source file
    @dp.table(name=f"{table}_quarantine", comment=f"{table} rows that failed silver rules")
    def quarantine():
        return spark.readStream.table(f"{table}_validated").filter(F.size("_failed_rules") > 0)

    #### Valid: rows that passed every rule
    @dp.temporary_view(name=f"{table}_valid")
    def valid():
        return spark.readStream.table(f"{table}_validated").filter(F.size("_failed_rules") == 0)

    #### Load: valid rows upserted by key, ordered by updated_at
    dp.create_streaming_table(name=table, comment=f"{table}: SCD{scd_type} by {key}")
    dp.create_auto_cdc_flow(
        target=table,
        source=f"{table}_valid",
        keys=[key],
        sequence_by=F.col("updated_at"),
        stored_as_scd_type=scd_type,
        except_column_list=DROP_COLUMNS,
    )


def build_event(table, key, casts, rules, cleanups=None):
    """Rows that never change: keep the earliest valid row per key, quarantine later copies.

    Delta doesn't enforce keys. Batch (materialized views), so the same row wins on every refresh.
    """
    #### Transform: validate, then mark later copies of an already valid key
    @dp.temporary_view(name=f"{table}_checked")
    @dp.expect_all(rules)
    def checked():
        bronze = spark.read.table(f"{catalog}.{bronze_schema}.{table}")
        df = validate(bronze, casts, rules, cleanups)
        passed = F.size("_failed_rules") == 0
        # Rank only rows that passed, so a broken row can't take the first spot
        first_seen = Window.partitionBy(F.col(key), passed).orderBy("created_at", "_ingested_at", "_source_file")
        is_copy = passed & (F.row_number().over(first_seen) > 1)
        return df.withColumn(
            "_failed_rules",
            F.when(is_copy, F.array(F.lit("duplicate_key"))).otherwise(F.col("_failed_rules")),
        )

    #### Quarantine: rows that broke a rule or repeat a key, with the reasons and source file
    @dp.materialized_view(name=f"{table}_quarantine", comment=f"{table} rows that failed silver rules")
    def quarantine():
        return spark.read.table(f"{table}_checked").filter(F.size("_failed_rules") > 0)

    #### Load: one row per key
    @dp.materialized_view(name=table, comment=f"{table}: one row per event")
    def load():
        return spark.read.table(f"{table}_checked").filter(F.size("_failed_rules") == 0).drop(*DROP_COLUMNS)


## Tables
# Per-table settings (key, append_only, scd_type, cleanups, casts, rules) live in
# config/sources.yml; every source with a silver block gets built here.
for table, source in SOURCES.items():
    if "silver" not in source:
        continue
    if source.get("append_only"):
        build_event(table, source["key"], **source["silver"])
    else:
        build_entity(table, source["key"], **source["silver"])
