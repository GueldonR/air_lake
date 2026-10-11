# AirLake

Databricks Asset Bundle for a medallion architecture data pipeline

## Requirements

- Databricks workspace with Unity Catalog enabled.
- Databricks CLI installed and authenticated.
- Permission to create and write to the configured catalog, schemas, and volumes.
- Sample files in `data/sample`.
- A Unix-style shell with `make` (Linux or macOS terminal; on Windows use Git Bash or WSL).

## Configure

Set workspace/catalog-specific values in `databricks.yml`. The bundle passes them to the
setup, ingestion, silver, and gold resources.

## Deploy

From the project root:

```powershell
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

Run the deployed resources in this order:

1. `AirLake-dev-setup` creates schemas, volumes, and raw-data folders.
2. `AirLake-dev-load-sample` copies sample CSVs (deployed with the bundle) into the raw
   folders. Parameter `dataset`: `initial` (default), `incremental` or `scd`.
3. `AirLake-dev-ingestion-job` loads CSV files into bronze Delta data.
4. `AirLake-dev-silver-pipeline` creates silver tables and the business view.
5. `AirLake-dev-gold-dimensions` updates gold dimensions, then the fact table.

The jobs use serverless compute. Databricks Free Edition may limit concurrent or
active serverless runs; stop unused jobs and pipeline updates before retrying.

## Quick start with make

```text
make all          # deploy, setup, load initial data, run bronze -> silver -> gold
make incremental  # load incremental files and rerun the pipeline
make scd          # load SCD change files and rerun the pipeline
make run          # rerun the pipeline only
```

## Data Flow

```mermaid
flowchart LR
    CSV[data/sample CSV files] --> RAW[Raw volume]
    RAW --> ING[Ingestion job]
    ING --> BRONZE[Bronze Delta data]
    BRONZE --> SILVER[Silver pipeline]
    SILVER --> TABLES[Silver tables and business view]
    TABLES --> DIMS[Gold dimensions]
    DIMS --> FACT[Gold fact table]
```

## Layout

```text
databricks.yml       Bundle variables and targets
resources/           Jobs and pipeline definitions
src/notebooks/       Setup, ingestion, and gold notebooks
src/Pipelines/       Silver tables and views
data/sample/<dataset>/<table>/   Sample CSVs (initial, incremental, scd), mirrors raw volume
```
