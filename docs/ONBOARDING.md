# Onboarding — adopt this for your institution

A Databricks-native Enable Data Union: pull your Ed-Fi data into a governed dimensional warehouse on one platform (no Airflow, dbt, or Snowflake). This takes you from zero to a running warehouse against your own ODS.

## 1. Prerequisites
- A Databricks workspace with **Unity Catalog** and **serverless** enabled.
- The Databricks CLI authenticated to it (`databricks auth login`).
- Either an **Ed-Fi ODS/API** endpoint (your district/state's), or nothing — run in `synthetic` mode to see the whole thing work offline first.

## 2. Know your ODS (answer these first)
1. **Ed-Fi ODS/API version + Data Standard version**, and do you use Ed-Fi **Profiles**? — drives field mapping, especially demographics.
2. **Which domains do you populate?** attendance, programs (SpEd / EL / Title I), assessments, discipline, TPDM.
3. **Any extensions or custom descriptors?**
4. **API access:** base URL + a client key/secret.

> Demographics note: race/sex mapping is validated for Data Standard 5.x. If your ODS is on a newer model (or restricts fields via a Profile), confirm where demographics live and adjust the recipe — the pipeline degrades gracefully (race → "Unknown") rather than failing.

## 3. Configure (`databricks.yml` variables)
- `catalog`, `bronze_schema`, `silver_schema`, `gold_schema` — your Unity Catalog names.
- `tenant_code`, `api_year` — tags stamped on every row (supports multiple districts/years in one catalog).
- `data_mode` — `synthetic` (offline), or `sandbox` / `custom` (real API).
- **Credentials:** put `edfi_client_key` / `edfi_client_secret` in a Databricks **secret scope** and set `edfi_secret_scope` (preferred over the plaintext variables, which are visible in the job UI).
- **Tunables (no code change):** `chronic_absence_threshold` / `chronic_absence_min_days`, and the `multiple_races_code` / `hispanic_latino_code` / `race_unknown_code` labels.
- **Per-developer overrides:** set your workspace via your CLI profile (`-p <profile>` / `PROFILE=<profile>`), and override variable defaults with `BUNDLE_VAR_<name>` env vars or a gitignored `*.local.env` you `source` before running `make`.

## 4. Run
- `make deploy` then `make run` — start in **synthetic** mode: proves the full pipeline end-to-end with no credentials.
- `make smoke` — a fast subset pull from a real ODS (defaults to the Ed-Fi public sandbox); ~8 min.
- **Full real pull:** set `data_mode=custom`, `edfi_base_url`, and `edfi_secret_scope`, then run the job.

## 5. Reconcile (this is acceptance)
A green run proves the pipeline is *structurally* correct (keys unique, FKs resolve, invariants hold — see `qc_scorecard`). It does **not** prove the numbers match your reality. Before trusting it, compare a few Gold figures — total enrollment, chronic-absenteeism %, program participation counts — against your existing state/district reports.

## 6. Customize
- **Add a field:** one line in `src/silver/recipes.py`.
- **Add a mart:** one `@dlt.table` in `src/gold/pipeline.py` + a row in `notebooks/99_run_e2e_tests.py`.
- **State-specific business rules:** bundle variables (above).
- Full code tour: **`docs/ENGINEERING_OVERVIEW.md`**.
