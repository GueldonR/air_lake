clean:
# Remove the local bundle cache so deploy re-uploads every file
ifeq ($(OS),Windows_NT)
	powershell -NoProfile -Command "Remove-Item -Recurse -Force -ErrorAction SilentlyContinue .databricks"
else
	rm -rf .databricks
endif

deploy:
# Deploy jobs, pipeline and sample data to the workspace
	databricks bundle deploy

setup:
# Create schemas, volumes and raw folders
	databricks bundle run setup

load:
# Copy the initial sample CSVs into the raw folders
	databricks bundle run load_sample --params dataset=initial

run:
# Run the pipeline (bronze + silver), then gold
	databricks bundle run airlake_pipeline
	databricks bundle run gold_dimensions

full-refresh:
# Rebuild every pipeline table from scratch, then gold
	databricks bundle run airlake_pipeline --full-refresh-all
	databricks bundle run gold_dimensions

all: clean deploy setup load run
# First-time run: everything in order

incremental:
# Load incremental files, then rerun the pipeline
	databricks bundle run load_sample --params dataset=incremental
	$(MAKE) run

scd:
# Load SCD change files, then rerun the pipeline
	databricks bundle run load_sample --params dataset=scd
	$(MAKE) run

destroy:
# Remove deployed jobs, pipelines and files (keeps schemas and data)
	databricks bundle destroy --auto-approve
