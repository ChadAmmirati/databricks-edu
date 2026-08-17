# MART_PARITY — EDU `edu_wh` model inventory & status

This is the source of truth for which EDU `core_warehouse` and `tpdm_warehouse` models are built in this project, scaffolded, or still TODO. Every row that says **BUILT** has a 1:1 PySpark equivalent in `src/gold/pipeline.py`.

> **Note on parity:** Silver staging is **already at 100% parity** with `edu_edfi_source` because it's metadata-driven (one `resources.yml` entry → one `*_base` + one stage table). The work tracked here is purely the gold dimensional/fact layer.

## Surrogate-key conventions

Every gold mart joins on these keys, which are computed identically to EDU's `dbt_utils.generate_surrogate_key()` (lowercased MD5 of concatenated parts). See `src/gold/shared/keys.py`.

## `core_warehouse` dimensions (22 total, 22 built)

| EDU model                | Status      |
| ------------------------ | ----------- |
| `dim_assessment`         | **BUILT**   |
| `dim_calendar_date`      | **BUILT**   |
| `dim_class_period`       | **BUILT**   |
| `dim_classroom`          | **BUILT**   |
| `dim_cohort`             | **BUILT**   |
| `dim_course`             | **BUILT**   |
| `dim_course_section`     | **BUILT**   |
| `dim_esc`                | **BUILT**   |
| `dim_grading_period`     | **BUILT**   |
| `dim_graduation_plan`    | **BUILT**   |
| `dim_lea`                | **BUILT**   |
| `dim_learning_standard`  | **BUILT**   |
| `dim_network`            | **BUILT**   |
| `dim_objective_assessment` | **BUILT** |
| `dim_parent`             | **BUILT**   |
| `dim_program`            | **BUILT**   |
| `dim_school`             | **BUILT**   |
| `dim_school_calendar`    | **BUILT**   |
| `dim_session`            | **BUILT**   |
| `dim_staff`              | **BUILT**   |
| `dim_student`            | **BUILT**   |
| `dim_subgroup`           | **BUILT**   | Delivered as `fct_student_subgroup` (long-table form); membership counts **active** participation only (exited students excluded). EDU's wide-table form can be flipped on request. |

## `core_warehouse` facts (34 total, 34 built)

| EDU model                                                          | Status     |
| ------------------------------------------------------------------ | ---------- |
| `brg_course_section_program`                                       | **BUILT**  |
| `fct_course_transcripts`                                           | **BUILT**  |
| `fct_staff_school_association`                                     | **BUILT**  |
| `fct_staff_section_association`                                    | **BUILT**  |
| `fct_student_academic_record`                                      | **BUILT**  |
| `fct_student_assessment`                                           | **BUILT**  |
| `fct_student_cohort_association`                                   | **BUILT**  |
| `fct_student_cte_program_associations`                             | **BUILT**  |
| `fct_student_daily_attendance`                                     | **BUILT**  |
| `fct_student_diploma`                                              | **BUILT**       |
| `fct_student_discipline_actions`                                   | **BUILT**  |
| `fct_student_discipline_actions_summary`                           | **BUILT**  |
| `fct_student_discipline_incident_behaviors`                        | **BUILT**  |
| `fct_student_discipline_incident_non_offenders`                    | **BUILT**  |
| `fct_student_discipline_incident_summary`                          | **BUILT**  |
| `fct_student_gpa`                                                  | **BUILT**  |
| `fct_student_grades`                                               | **BUILT**  |
| `fct_student_homeless_program_association`                         | **BUILT**  |
| `fct_student_language_instruction_program_association`             | **BUILT**  |
| `fct_student_learning_standard_grades`                             | **BUILT**       |
| `fct_student_migrant_education_program_associations`               | **BUILT**  |
| `fct_student_objective_assessment`                                 | **BUILT**       |
| `fct_student_parent_association`                                   | **BUILT**  |
| `fct_student_program_association`                                  | **BUILT**  |
| `fct_student_program_service`                                      | **BUILT**       |
| `fct_student_school_association`                                   | **BUILT**  |
| `fct_student_school_attendance_event`                              | **BUILT**  |
| `fct_student_school_food_service_program_associations`             | **BUILT**  |
| `fct_student_section_association`                                  | **BUILT**  |
| `fct_student_section_attendance_event`                             | **BUILT**  |
| `fct_student_special_education_program_association`                | **BUILT**  |
| `fct_student_subgroup`                                              | **BUILT**  |
| `fct_student_title_i_part_a_program_association`                   | **BUILT**  |
| `msr_student_cumulative_attendance`                                | **BUILT**  |

## `tpdm_warehouse` (Teacher Preparation Data Model)

| EDU model                                                        | Status |
| ---------------------------------------------------------------- | ------ |
| `dim_candidate`                                                  | **BUILT**   |
| `dim_educator_preparation_program`                               | **BUILT**   |
| `fct_candidate_educator_preparation_program_association`         | **BUILT**   |

## Summary

| Layer                 | EDU model count | Built |
| --------------------- | --------------- | ----- |
| Silver (staging)      | 269             | 269 (metadata-driven, full parity) |
| Gold — `core_warehouse` dims | 22         | 22    |
| Gold — `core_warehouse` facts | 34        | 34    |
| Gold — `tpdm_warehouse`      | 3          | 3     |
| **Total**             | **328**         | **328** |

**Overall parity: the full EDU gold surface area is built** (58 marts in `src/gold/pipeline.py`). Remaining work is business-rule fidelity and optional extensions (e.g. additional state-reporting marts), not missing tables — see "Business-rule enhancements" and "Remaining work" below.


## Business-rule enhancements (closed gaps)

Built on top of the structural warehouse, each with a unit test and an end-to-end gate in `notebooks/99_run_e2e_tests.py`, and verified end-to-end on the Ed-Fi public sandbox. These close gaps previously listed under "Known divergences":

| Enhancement | What it does |
| --- | --- |
| **Descriptor label resolution** | Ed-Fi `*Descriptors` endpoints → a Silver `_descriptors` lookup; descriptor columns resolve to `shortDescription` instead of raw codes. Key-feeding descriptors stay as `codeValue` for surrogate-key parity. |
| **Chronic absenteeism** | `is_chronically_absent` on `msr_student_cumulative_attendance` (tunable: present < 90% over ≥ 20 days). |
| **Program active/annual inclusion** | `is_active`/`is_annual` on all 7 program-association facts; `fct_student_subgroup` counts **active** membership only, so exited students no longer inflate SpEd/EL/Title-I counts. |
| **Demographic race/ethnicity rollup** | `race_ethnicity` on `dim_student` (federal rollup: Hispanic→Latinx, multi-race→Multiple, none→Unknown; tunable labels). |

## Known divergences from upstream EDU

Documented differences vs `edu_edfi_source`/`edu_wh` — review with your adopting agency before promising literal parity on these:

| Area | Upstream EDU | This project |
| --- | --- | --- |
| Null FK references | `gen_skey` yields a NULL key when the reference is null | Key hashes the dbt-utils null marker (non-null key). Joins on missing refs behave differently. |
| Date inputs to keys | Cast `::string::date` before hashing (strips timestamps) | Hashed as extracted strings. Identical when the API returns bare dates (it does); diverges if a source system appends timestamps. |
| Descriptor labels | `_descriptors` table + configurable code→description mapping | ✅ Resolved — `_descriptors` lookup resolves codes to `shortDescription` (see Business-rule enhancements). |
| Attendance semantics | `xwalk_attendance_events` seed + chronic-absence config (90% / 20 days), N-events→school-day rules | ✅ Chronic-absence flag added (90%/20-day, tunable). `is_absent` = category matches `(?i)Absence`; the N-events→school-day xwalk is still simplified. |
| Grade-level ordering | `xwalk_grade_levels` seed (grade→integer) | Raw descriptor strings, no ordering column. |
| Enrollment edge rules | Configurable withdraw-code exclusions, exit-before-first-day exclusion, year-end extension window | Program associations now carry `is_active`/`is_annual`; **student-enrollment** withdraw-code exclusions on `fct_student_school_association` are still open (see Remaining work). |
| Academic-term ordering | `xwalk_academic_terms` seed | Fixed Fall<Winter<Spring<Summer ordering in `fct_student_gpa`. |
| Nested-array child tables | `stg_ef3__students__addresses` etc. per array | Arrays stay in `payload`; gold explodes the ones marts need (diplomas, GPAs, services, learning-standard grades, objective assessments, section programs). |
| keyChanges endpoint | Pulled + applied | Client supports it (`get_key_changes`); not yet wired into the extractor. Natural-key renames re-land under the new key; the old row is removed only if the ODS also emits a delete. |
| Cursor paging (ODS >= 7.3) | `Next-Page-Token` paging | Change-version window stepping + reverse-offset paging (equivalent miss-protection, more requests on very large pulls). |

## DS-version handling

Validated against the **Ed-Fi 7.3 / Data Standard 5.x** OpenAPI spec (the version the public sandbox runs):

- `parents` / `studentParentAssociations` / `studentDisciplineIncidentAssociations` do not exist on DS 5.x — marked `optional` in the manifest (404 → skipped) and bridged in gold (`dim_parent` ⇐ parents ∪ contacts; incident participation ⇐ legacy ∪ behavior associations).
- Grading-period keys use `gradingPeriodName` (DS 5.x) with descriptor fallback for older ODSes.
- GPA extracted from `gradePointAverages[]` (DS 5.x) with flat-field fallback.
- Discipline actions use `responsibilitySchoolReference` (no plain `schoolReference` on DS 5.x).
- Attendance date field is `eventDate`.

## Remaining work

Structural marts are complete — including the four facts (`fct_student_diploma`, `fct_student_learning_standard_grades`, `fct_student_objective_assessment`, `fct_student_program_service`) and the three TPDM marts once listed here as TODO; all are now built. Open items are business-rule fidelity and optional extensions:

- **Enrollment edge rules** — configurable withdraw-code exclusions (`No show`, `Invalid enrollment`), exclude exits before the first instructional day, and the year-end extension window on `fct_student_school_association`.
- **Grade-level ordering** — a grade→integer mapping (EDU `xwalk_grade_levels`) for sorting and cohort math.
- **Descriptor coverage QC on real data** — flag descriptor codes present in the data but missing from the ODS's descriptor tables (synthetic is fully covered; a real ODS may not be).
- **Net-new optional marts** — `fct_student_characteristics`, `fct_student_district`, the staff employment/assignment/contact trio, etc. (common in state-reporting extensions).
- **Real Ed-Fi ODS validation** — run against the public Ed-Fi sandbox / a real ODS (where `codeValue` ≠ `shortDescription`) before production rollout.

## How to add a new mart

1. **Identify the silver tables** the EDU dbt model `ref()`s. Most facts pull from one stage table + the surrogate keys of the related dims.
2. **Add an `@dlt.table` def** to `src/gold/pipeline.py`. Use the helpers in `gold/shared/keys.py` so your keys match upstream EDU exactly.
3. **Add a row** to `notebooks/99_run_e2e_tests.py :: GOLD_TABLES` (and the FK section if it's a fact).
4. **Update this file** — flip the status to **BUILT**.
5. `databricks bundle deploy && databricks bundle run edu_ingestion_job` to validate.
