# AirLake

Databricks Asset Bundle for a medallion architecture data pipeline

## Requirements

- Databricks workspace with Unity Catalog enabled.
- Databricks CLI installed and authenticated.
- Permission to create and write to the configured catalog, schemas, and volumes.
- Sample data unzipped into `data/sample` (see [Sample data](#sample-data)).
- A Unix-style shell with `make` (Linux or macOS terminal; on Windows use Git Bash or WSL).

## Sample data

The sample data ships as `data/sample_.zip`. Unzip it before deploying, so the bundle
uploads the CSVs to the workspace:

```bash
uv run python -m zipfile -e data/sample_.zip data/              # any OS
Expand-Archive data/sample_.zip -DestinationPath data/           # PowerShell
```

This creates `data/sample/<dataset>/<table>/` with the datasets `initial`, `incremental`,
`scd` and `dirty`. The unzipped folder is not committed; only the zip is. The tables are
described in [docs/source_schema.md](docs/source_schema.md).

## Configure

Set workspace/catalog-specific values in `databricks.yml`. The bundle passes them to the
setup job, the pipeline, and the gold job.

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
3. `AirLake-dev-pipeline` ingests the raw CSVs into bronze tables with Auto Loader, then
   builds the silver tables and the business view.
4. `AirLake-dev-gold-dimensions` updates gold dimensions, then the fact table.

The jobs use serverless compute. Databricks Free Edition may limit concurrent or
active serverless runs; stop unused jobs and pipeline updates before retrying.

## Quick start with make

```text
make all          # deploy, setup, load initial data, run bronze -> silver -> gold
make incremental  # load incremental files and rerun the pipeline
make scd          # load SCD change files and rerun the pipeline
make run          # rerun the pipeline only
make full-refresh # rebuild all pipeline tables from scratch, then gold
```

## Data Flow

```mermaid
flowchart LR
    CSV[data/sample CSV files] --> RAW[Raw volume]
    subgraph PIPE[Pipeline]
        BRONZE[Bronze tables - Auto Loader] --> TABLES[Silver tables and business view]
    end
    RAW --> BRONZE
    TABLES --> DIMS[Gold dimensions]
    DIMS --> FACT[Gold fact table]
```

## Layout

```text
databricks.yml       Bundle variables and targets
resources/           Jobs and pipeline definitions
src/notebooks/       Setup, sample-load, and gold notebooks
src/Pipelines/       Pipeline source: bronze ingestion, silver tables and views
data/sample_.zip     Sample data; unzips to data/sample/<dataset>/<table>/ (mirrors raw volume)
docs/                Source schema
```
