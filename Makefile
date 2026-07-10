PROFILE ?= DEFAULT
TF      := DATABRICKS_TF_EXEC_PATH=$(shell command -v terraform 2>/dev/null)

.PHONY: help validate deploy run run-async smoke clean test unit lint fmt

help:
	@echo "Common targets:"
	@echo "  make validate      # databricks bundle validate"
	@echo "  make deploy        # databricks bundle deploy"
	@echo "  make run           # databricks bundle run edu_ingestion_job"
	@echo "  make unit          # local pytest unit tests"
	@echo "  make test          # unit + bundle validate"
	@echo "  make smoke         # fast real-data smoke test vs the Ed-Fi public sandbox (edu_*_smoke)"
	@echo ""
	@echo "Override the target workspace with PROFILE=<name>."

validate:
	$(TF) databricks bundle validate -p $(PROFILE)

deploy:
	$(TF) databricks bundle deploy --auto-approve -p $(PROFILE)

run:
	$(TF) databricks bundle run edu_ingestion_job -p $(PROFILE)

run-async:
	$(TF) databricks bundle run edu_ingestion_job --no-wait -p $(PROFILE)

# --- Fast real-data smoke test against the Ed-Fi public sandbox ---
# Pulls a small subset of resources (+ all descriptors) into isolated edu_*_smoke
# schemas, validating the real-data path in ~8 min instead of a full pull. Creds
# below are the PUBLIC Ed-Fi sample-sandbox credentials (read-only demo data).
# NOTE: repoints the bundle to *_smoke schemas — run `make deploy` to restore defaults.
EDFI_SANDBOX_URL    ?= https://api.ed-fi.org/v7.3.2/api
EDFI_SANDBOX_KEY    ?= populatedKey
EDFI_SANDBOX_SECRET ?= populatedSecret
SMOKE_RESOURCES     ?= local_education_agencies|schools|sessions|students|staffs|courses|course_offerings|sections|programs|student_school_associations|staff_school_associations|student_education_organization_associations|student_special_education_program_associations

smoke:
	$(TF) databricks bundle deploy -p $(PROFILE) \
		--var="bronze_schema=edu_bronze_smoke" --var="silver_schema=edu_silver_smoke" --var="gold_schema=edu_gold_smoke"
	$(TF) databricks bundle run edu_ingestion_job -p $(PROFILE) \
		--var="bronze_schema=edu_bronze_smoke" --var="silver_schema=edu_silver_smoke" --var="gold_schema=edu_gold_smoke" \
		--params "data_mode=sandbox,edfi_base_url=$(EDFI_SANDBOX_URL),edfi_client_key=$(EDFI_SANDBOX_KEY),edfi_client_secret=$(EDFI_SANDBOX_SECRET),full_refresh=true,resources_filter=$(SMOKE_RESOURCES)"

unit:
	@python -m pytest tests/unit -q

test: unit validate

clean:
	rm -rf .databricks/ .venv/ **/__pycache__/ .pytest_cache/
