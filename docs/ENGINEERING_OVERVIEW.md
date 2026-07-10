# Engineering overview

A click-through tour of the codebase — what every piece does and where it lives — so you can answer questions without spelunking. Layers run Bronze → Silver → Gold → Quality, orchestrated by a Lakeflow Job and deployed by an Asset Bundle. It replaces Enable Data Union's Airflow + dbt + Snowflake with Lakeflow Spark Declarative Pipelines (SDP) + Unity Catalog.

## Repo map
| Path | What |
| --- | --- |
| `src/edfi_extract/` | **Bronze** — Ed-Fi API client, extractor, manifest, synthetic generator |
| `src/silver/` | **Silver** — extraction recipes, reusable transforms, SDP pipeline |
| `src/gold/` | **Gold** — dimensional marts, surrogate keys, QC tables |
| `resources/*.yml` | The Lakeflow Job + the two SDP pipelines |
| `databricks.yml` | Asset Bundle: targets + variables |
| `notebooks/` | Thin entrypoints (00 bronze, 10 silver, 20 gold, 99 e2e) + `90_demo_queries.sql` |

## 1. Bronze — extraction (`src/edfi_extract/`)
- **`client.py → EdFiClient`** — our minimal Ed-Fi API client (no vendor dependency). OAuth2 client-credentials with auto-refresh (`_refresh_token`); pulls resources (`get_resource`), `/deletes`, `/keyChanges`; reads the newest change version (`get_latest_change_version`).
- **`extractor.py → extract_all`** — loops the manifest, lands each resource to `{name}_raw` (raw JSON in a `payload` column). Holds the incremental logic, watermarks, and the smoke-test `resources_filter`.
- **`resources.yml`** — the manifest, the source of truth for what we pull: one entry per Ed-Fi endpoint (`path`, `key_paths`, `deletes`, `optional`). ~73 data resources + 48 descriptor endpoints.
- **`synthetic.py → generate_synthetic_dataset`** — deterministic fake Ed-Fi data shaped to the 7.3 spec, so the whole pipeline runs offline/CI with no ODS.

## 2. Silver — staging (`src/silver/`)
- **`recipes.py → RECIPES`** — maps each Silver column → a JSON path into `payload` (e.g. `"student_unique_id": "$.studentReference.studentUniqueId"`). Adding a field = one line here.
- **`transforms.py`** — the reusable, unit-tested PySpark logic: `surrogate_key`, `dedupe_latest`, `extract_descriptor`, `resolve_descriptors`, `chronic_absent_flag`, `program_window_flags`, `race_ethnicity_rollup`.
- **`pipeline.py`** — the SDP pipeline. For each manifest resource it builds `{name}_base` (typed columns via the recipe) and `{name}` (deduped by surrogate key, `/deletes` anti-joined out). Also builds the **`_descriptors`** lookup and resolves descriptor codes → labels here.

## 3. Gold — warehouse (`src/gold/`)
- **`pipeline.py`** — 58 `@dlt.table` marts (23 dims, 33 facts, 1 measure, 1 bridge), each reading Silver via `_silver(name)`.
- **`shared/keys.py → _sk() + k_*`** — surrogate keys as `md5(lower(concat_ws('-', parts)))`, byte-identical to EDU's `dbt_utils.generate_surrogate_key()` so an EDU-trained analyst's joins work unchanged.
- **`qc.py`** — queryable `qc_*` tables: row counts, dim-PK uniqueness, fact-FK resolution, descriptor audit, business invariants, and a one-row `qc_scorecard`.

## 4. Quality gates
Inline SDP `@dlt.expect_*` annotations (real-time DQ) + `qc_*` tables (trendable) + **`notebooks/99_run_e2e_tests.py`**, the hard gate that fails the job unless silver isn't collapsed, deletes propagated, gold is populated, PKs unique, FKs resolve, and invariants hold.

## 5. Orchestration + deploy
- **`resources/ingestion_job.yml`** — the Lakeflow Job: `bronze_extract` → `silver_pipeline` → `gold_pipeline` → `e2e_tests`. All job parameters (catalog, schemas, `data_mode`, creds, `resources_filter`) live here.
- **`resources/{silver,gold}_pipeline.yml`** — the two serverless SDP pipelines; `configuration:` injects the tunable business-rule vars.
- **`databricks.yml`** — bundle targets + `variables:` (catalog, schemas, chronic/race tunables, Ed-Fi creds).

## Business-logic features (where + the key idea)
- **Descriptor resolution** — `_descriptors` lookup (Silver) + `resolve_descriptors`; `extract_descriptor` is **idempotent** so Gold's call sites resolve labels with zero changes. Key-feeding descriptors stay as `codeValue` (a 3-name deny-list) so surrogate keys still match EDU.
- **Chronic absenteeism** — `chronic_absent_flag` → `is_chronically_absent` on `msr_student_cumulative_attendance` (tunable 90% / 20 days).
- **Program active/annual** — `program_window_flags` → `is_active`/`is_annual` on the 7 program facts; `fct_student_subgroup` counts **active** only (exited students don't inflate counts).
- **Demographic rollup** — `race_ethnicity_rollup` → `dim_student.race_ethnicity` (Hispanic→Latinx / multi→Multiple / none→Unknown; coalesced so never NULL).

## Advanced points (likely questions)
- **Surrogate-key parity:** md5 of lowercased, `-`-joined, null-markered parts — identical to dbt_utils, so it joins to EDU.
- **Incremental + safety:** one change-version snapshot per run is the `maxChangeVersion` for every resource; per-(tenant, year, resource) watermarks; a snapshot going *backwards* hard-errors (ODS restored from backup).
- **Reverse-offset paging:** pages walked end→0 so rows shifting mid-pull cause *duplicates* (Silver dedupes) instead of silent misses.
- **Deletes:** `/deletes` tombstones land in `_deletes_raw`, anti-joined out in Silver staging.
- **Synthetic integrity:** the generator records every descriptor it emits, so the lookup provably covers every code; tests fail on hollow data.
- **`data_mode`-scoped gates:** "distribution" gates (a realistic value mix) run only on synthetic; correctness gates (keys/FKs/ranges/no-nulls) run in every mode.

## How to extend
- **Field:** one line in `src/silver/recipes.py`.
- **Mart:** one `@dlt.table` in `src/gold/pipeline.py` + a row in `notebooks/99_run_e2e_tests.py :: GOLD_TABLES`.
- **State-specific rule:** a bundle variable (no code change).
