# Real-data validation — Ed-Fi public sandbox (v7.3.2)

**Status:** ✅ Full pipeline green end-to-end against the live Ed-Fi sandbox (2026-06-25).

Before this, the project was only ever run against the synthetic generator. This validates it against a **real Ed-Fi ODS/API** with real data shapes — the gate before any production rollout.

## What was run
- **Endpoint:** `https://api.ed-fi.org/v7.3.2/api` (Ed-Fi ODS/API 7.3.2, **Data Model 6.1**), "populated" sandbox (Grand Bend sample, ~1,959 students).
- **Mode:** `data_mode=sandbox`, public sample creds (`populatedKey`/`populatedSecret`), isolated `edu_*_sbx_live` schemas, `full_refresh` for the initial pull.
- **Result:** `bronze_extract` → `silver_pipeline` → `gold_pipeline` → `e2e_tests` all **SUCCESS**; e2e `failures: []`.

## What works on real data (not just synthetic)
- **Descriptor resolution** produces real labels: `term` = Fall/Spring Semester; `academic_subject` = English Language Arts, Fine and Performing Arts, …; attendance categories incl. a real-only **"Partial"**; staff education = Bachelor's/Doctorate/…; program type = Bilingual/Gifted and Talented/… — zero `uri://` leaks.
- **Chronic absenteeism** computes on real attendance: **15 of 227** students with attendance (6.6%).
- **Program active/annual**: 65 SpEd associations; `active == annual` (Grand Bend has no mid-year exits in-window — handled correctly by the universal `active ≤ annual` gate).
- **Universal correctness gates all pass on real data**: PK uniqueness, FK resolution, attendance/`is_absent` consistency, `enrollment_days ≥ 0`, GPA in-range, descriptor no-leaks, race no-nulls, silver-not-collapsed.

## Findings & fixes (made during validation)
1. **Phantom resource removed.** `staffEducationOrganizationContactAssociations` was marked *required* in the manifest but isn't a real Ed-Fi resource (404; only the synthetic generator fabricated it; nothing in gold consumed it). Found by a **mechanical diff** of our manifest against the ODS's 471 advertised resources — that was the *only* required phantom. The other 6 manifest resources absent from the sandbox are correctly `optional` and skip gracefully (`parents`/`studentParentAssociations`/`studentDisciplineIncidentAssociations` — removed in DS 5.x; the 3 `tpdm/*` — TPDM not enabled on this sandbox).
2. **`race_ethnicity` null fix.** Students with no `studentEducationOrganizationAssociation` slipped through the left join as NULL. Now coalesced to the unknown label — a total rollup over every student.
3. **e2e gates scoped by `data_mode`.** "Distribution" gates that assert *our synthetic data's shape* (deletes present, nested arrays populated, GPA always present, a chronic/multi-race mix) now run **only in synthetic mode**. Universal correctness gates run in every mode.

## Open finding for adopters (not a bug)
**Demographics did not populate on the DS-6.1 sandbox.** On this ODS, `studentEducationOrganizationAssociation` carries **no** `sexDescriptor` / `races` / `hispanicLatinoEthnicity` (the records we read carried internet-access/learning-device/cohort indicators instead). Cause unconfirmed — likely an Ed-Fi **Profile** restriction on the public sandbox key, a Data-Standard-version difference, or the sample simply not populating them. Our recipe targets the **DS-5.x** location, so on the sandbox `dim_student.gender` is empty and `race_ethnicity` is "Unknown" for all students. The pipeline degraded gracefully (no nulls, no crash).

**Action before relying on race/gender:** confirm your institution's Ed-Fi **Data Standard version**. The default mapping is correct for DS 5.x (validated in synthetic); if your ODS is DS 6.x, re-source the demographic fields from their DS-6.x location. (The actual ODS version should be confirmed — your data, not the public sample, is the real target.)

## How to re-run
```
databricks bundle run edu_ingestion_job -p <profile> \
  --var=bronze_schema=edu_bronze_sbx_live --var=silver_schema=edu_silver_sbx_live --var=gold_schema=edu_gold_sbx_live \
  --params data_mode=sandbox,edfi_base_url=https://api.ed-fi.org/v7.3.2/api,edfi_client_key=populatedKey,edfi_client_secret=populatedSecret,full_refresh=false
```
**Fast smoke test** — a subset of resources (+ descriptors) into isolated `edu_*_smoke` schemas, ~8 min, for iterating or a demo walkthrough:
```
make smoke
```
This uses the `resources_filter` bronze param (pipe-separated resource names; empty = all) to pull only a core subset while still building every Silver/Gold table.

For your institution's ODS, put the client key/secret in a Databricks **secret scope** and pass `edfi_secret_scope=<scope>` instead of the plaintext key/secret params.
