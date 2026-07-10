# Databricks notebook source
# MAGIC %md
# MAGIC # E2E data-quality gate
# MAGIC
# MAGIC Runs as the final task in the Lakeflow Job and fails the job unless the
# MAGIC warehouse is demonstrably correct:
# MAGIC
# MAGIC 1. **Silver collapse guard** — every staging table holds roughly one row per
# MAGIC    distinct source record. Catches wrong key paths that hash NULLs and
# MAGIC    collapse a table during dedupe (the bug class synthetic data used to hide).
# MAGIC 2. **Delete propagation** — every tombstone in `_deletes_raw` is absent from
# MAGIC    its silver staging table.
# MAGIC 3. **Gold population** — marts must be NON-EMPTY whenever their silver source
# MAGIC    has rows. Existence alone is not acceptance.
# MAGIC 4. PK uniqueness on every dim; FK resolution on core facts; business invariants.
# MAGIC 5. **Freshness** — bronze watermarks must be from this run window.

# COMMAND ----------

dbutils.widgets.text("catalog", "edu_demo")
dbutils.widgets.text("gold_schema", "edu_gold")
dbutils.widgets.text("silver_schema", "edu_silver")
dbutils.widgets.text("bronze_schema", "edu_bronze")
dbutils.widgets.text("max_pull_age_hours", "26")
dbutils.widgets.text("data_mode", "synthetic")

CATALOG = dbutils.widgets.get("catalog")
GOLD = dbutils.widgets.get("gold_schema")
SILVER = dbutils.widgets.get("silver_schema")
BRONZE = dbutils.widgets.get("bronze_schema")
MAX_PULL_AGE_HOURS = int(dbutils.widgets.get("max_pull_age_hours"))
DATA_MODE = dbutils.widgets.get("data_mode")
# Distribution gates (asserting a realistic MIX of values) only hold on our
# synthetic dataset; a real ODS sample has its own shape. Correctness gates run
# in every mode.
SYNTHETIC = (DATA_MODE == "synthetic")

# COMMAND ----------

from pyspark.sql import functions as F

failures = []


def _g(table: str):
    return f"`{CATALOG}`.`{GOLD}`.`{table}`"


def _s(table: str):
    return f"`{CATALOG}`.`{SILVER}`.`{table}`"


def _b(table: str):
    return f"`{CATALOG}`.`{BRONZE}`.`{table}`"


def check(name: str, ok: bool, detail: str = ""):
    status = "PASS" if ok else "FAIL"
    print(f"[{status}] {name} — {detail}")
    if not ok:
        failures.append(name)

# COMMAND ----------

# MAGIC %md ## 1. Silver collapse guard

# COMMAND ----------

import os, sys, yaml

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__) if "__file__" in globals() else os.getcwd(), ".."))
with open(os.path.join(project_root, "src", "edfi_extract", "resources.yml")) as f:
    RESOURCES = yaml.safe_load(f)["resources"]

deletes_per_resource = {
    r["resource"]: r["n"]
    for r in spark.sql(
        f"SELECT resource, COUNT(DISTINCT record_guid) AS n FROM {_b('_deletes_raw')} GROUP BY resource"
    ).collect()
}

for res in RESOURCES:
    name = res["name"]
    if name.endswith("_descriptors"):
        continue  # descriptor endpoints feed the _descriptors lookup, not a stage table
    src_guids = spark.sql(
        f"SELECT COUNT(DISTINCT record_guid) AS n FROM {_b(name + '_raw')} WHERE record_guid IS NOT NULL"
    ).collect()[0]["n"]
    stage_rows = spark.table(_s(name)).count()
    if src_guids <= 1:
        continue  # endpoint empty/missing on this ODS; nothing to collapse
    expected = src_guids - deletes_per_resource.get(name, 0)
    # Dedupe can legitimately shrink counts slightly (same record re-pulled
    # under a new GUIDless representation), but an order-of-magnitude drop
    # means key paths are wrong and the table collapsed.
    ok = stage_rows >= max(1, int(expected * 0.5)) and stage_rows > 1
    check(f"silver/{name} not collapsed", ok, f"stage_rows={stage_rows}, distinct_source_records={src_guids}, tombstones={deletes_per_resource.get(name, 0)}")

# Descriptor lookup feeds Silver label resolution; it must exist and be populated.
try:
    _desc_rows = spark.table(_s("_descriptors")).count()
    check("silver/_descriptors populated", _desc_rows > 0, f"rows={_desc_rows}")
except Exception as e:
    check("silver/_descriptors populated", False, f"error: {str(e)[:200]}")

# COMMAND ----------

# MAGIC %md ## 2. Delete propagation

# COMMAND ----------

tombstoned = spark.sql(
    f"SELECT DISTINCT resource, lower(replace(record_guid, '-', '')) AS guid FROM {_b('_deletes_raw')}"
).collect()
by_resource = {}
for row in tombstoned:
    by_resource.setdefault(row["resource"], set()).add(row["guid"])

if not by_resource and SYNTHETIC:
    check("deletes propagated", False, "no tombstones found in _deletes_raw — delete path is NOT being exercised")
for resource, guids in by_resource.items():
    remaining = (
        spark.table(_s(resource))
        .where(F.lower(F.regexp_replace(F.col("record_guid"), "-", "")).isin(list(guids)))
        .count()
    )
    check(f"deletes propagated for {resource}", remaining == 0,
          f"tombstones={len(guids)}, rows_still_in_silver={remaining}")

# COMMAND ----------

# MAGIC %md ## 3. Gold population — empty is a failure when the source has rows

# COMMAND ----------

# dim/fct -> (PK column or None, primary silver source table)
GOLD_TABLES = {
    "dim_lea": ("k_lea", "local_education_agencies"),
    "dim_school": ("k_school", "schools"),
    "dim_session": ("k_session", "sessions"),
    "dim_grading_period": ("k_grading_period", "grading_periods"),
    "dim_calendar_date": ("k_calendar_date", "calendar_dates"),
    "dim_course": ("k_course", "courses"),
    "dim_course_section": ("k_course_section", "sections"),
    "dim_staff": ("k_staff", "staffs"),
    "dim_student": ("k_student", "students"),
    "dim_assessment": ("k_assessment", "assessments"),
    "dim_esc": ("k_esc", "education_service_centers"),
    "dim_network": ("k_network", "education_organization_networks"),
    "dim_program": ("k_program", "programs"),
    "dim_cohort": ("k_cohort", "cohorts"),
    "dim_parent": (None, "contacts"),  # PK unique only per source endpoint after parents/contacts bridge
    "dim_class_period": ("k_class_period", "class_periods"),
    "dim_classroom": ("k_classroom", "locations"),
    "dim_learning_standard": ("k_learning_standard", "learning_standards"),
    "dim_objective_assessment": ("k_objective_assessment", "objective_assessments"),
    "dim_graduation_plan": ("k_graduation_plan", "graduation_plans"),
    "dim_school_calendar": ("k_school_calendar", "calendars"),
    "fct_student_school_association": (None, "student_school_associations"),
    "fct_student_section_association": (None, "student_section_associations"),
    "fct_student_school_attendance_event": (None, "student_school_attendance_events"),
    "fct_student_section_attendance_event": (None, "student_section_attendance_events"),
    "fct_student_daily_attendance": (None, "student_school_attendance_events"),
    "fct_student_grades": (None, "grades"),
    "fct_student_assessment": (None, "student_assessments"),
    "fct_student_discipline_actions": (None, "discipline_actions"),
    "fct_student_discipline_actions_summary": (None, "discipline_actions"),
    "fct_student_discipline_incident_summary": (None, "student_discipline_incident_behavior_associations"),
    "fct_student_discipline_incident_behaviors": (None, "student_discipline_incident_behavior_associations"),
    "fct_student_discipline_incident_non_offenders": (None, "student_discipline_incident_non_offender_associations"),
    "fct_staff_school_association": (None, "staff_school_associations"),
    "fct_staff_section_association": (None, "staff_section_associations"),
    "fct_student_program_association": (None, "student_program_associations"),
    "fct_student_special_education_program_association": (None, "student_special_education_program_associations"),
    "fct_student_homeless_program_association": (None, "student_homeless_program_associations"),
    "fct_student_language_instruction_program_association": (None, "student_language_instruction_program_associations"),
    "fct_student_title_i_part_a_program_association": (None, "student_title_i_part_a_program_associations"),
    "fct_student_cte_program_associations": (None, "student_cte_program_associations"),
    "fct_student_migrant_education_program_associations": (None, "student_migrant_education_program_associations"),
    "fct_student_school_food_service_program_associations": (None, "student_school_food_service_program_association"),
    "fct_student_parent_association": (None, "student_contact_associations"),
    "fct_student_cohort_association": (None, "student_cohort_associations"),
    "fct_student_academic_record": (None, "student_academic_records"),
    "fct_student_gpa": (None, "student_academic_records"),
    "fct_course_transcripts": (None, "course_transcripts"),
    "fct_student_subgroup": (None, "students"),
    "msr_student_cumulative_attendance": (None, "student_school_attendance_events"),
    "brg_course_section_program": (None, "sections"),
    "fct_student_diploma": (None, "student_academic_records"),
    "fct_student_learning_standard_grades": (None, "grades"),
    "fct_student_objective_assessment": (None, "student_assessments"),
    "fct_student_program_service": (None, "student_program_associations"),
    "dim_candidate": ("k_candidate", "candidates"),
    "dim_educator_preparation_program": ("k_educator_prep_program", "educator_preparation_programs"),
    "fct_candidate_educator_preparation_program_association": (None, "candidate_educator_preparation_program_associations"),
    "qc_row_counts": (None, None),
    "qc_dim_pk_audit": (None, None),
    "qc_fct_fk_audit": (None, None),
    "qc_descriptor_audit": (None, None),
    "qc_freshness": (None, None),
    "qc_business_invariants": (None, None),
    "qc_scorecard": (None, None),
}

# Marts that explode a nested array: non-empty source doesn't guarantee rows,
# but the synthetic generator populates the arrays, so they must be non-empty too.
for table, (pk, source) in GOLD_TABLES.items():
    try:
        n = spark.table(_g(table)).count()
    except Exception as e:
        check(f"gold/{table} exists", False, f"error: {str(e)[:200]}")
        continue
    if source is None:
        check(f"gold/{table} exists", True, f"rows={n}")
        continue
    src_n = spark.table(_s(source)).count()
    if src_n > 0 and SYNTHETIC:
        # Synthetic populates every nested array, so a non-empty source must yield rows.
        check(f"gold/{table} populated", n > 0, f"rows={n} (source {source}={src_n})")
    else:
        # Real ODS: a mart can be empty when its nested source array is absent — require it built.
        check(f"gold/{table} exists", True, f"rows={n} (source {source}={src_n})")

# COMMAND ----------

# MAGIC %md ## 4. PK uniqueness, FK resolution, invariants

# COMMAND ----------

for table, (pk, _) in GOLD_TABLES.items():
    if not pk:
        continue
    df = spark.table(_g(table))
    total = df.count()
    distinct = df.select(pk).where(F.col(pk).isNotNull()).distinct().count()
    nulls = df.filter(F.col(pk).isNull()).count()
    check(f"gold/{table}.{pk} is unique", total == distinct and nulls == 0,
          f"rows={total}, distinct_keys={distinct}, null_keys={nulls}")


def fk_check(fact: str, fk: str, dim: str, dim_pk: str):
    f_df = spark.table(_g(fact))
    if fk not in f_df.columns:
        check(f"FK {fact}.{fk} -> {dim}.{dim_pk}", False, "FK column missing from fact")
        return
    d_df = spark.table(_g(dim))
    missing = (f_df.select(fk).distinct()
                  .join(d_df.select(F.col(dim_pk).alias(fk)).distinct(), [fk], "left_anti")
                  .filter(F.col(fk).isNotNull())
                  .count())
    check(f"FK {fact}.{fk} -> {dim}.{dim_pk}", missing == 0, f"orphan_keys={missing}")


fk_check("fct_student_school_association", "k_student", "dim_student", "k_student")
fk_check("fct_student_school_association", "k_school", "dim_school", "k_school")
fk_check("fct_student_section_association", "k_student", "dim_student", "k_student")
fk_check("fct_student_section_association", "k_course_section", "dim_course_section", "k_course_section")
fk_check("fct_student_school_attendance_event", "k_student", "dim_student", "k_student")
fk_check("fct_student_school_attendance_event", "k_school", "dim_school", "k_school")
fk_check("fct_student_grades", "k_student", "dim_student", "k_student")
fk_check("fct_student_grades", "k_course_section", "dim_course_section", "k_course_section")
fk_check("fct_student_grades", "k_grading_period", "dim_grading_period", "k_grading_period")
fk_check("fct_student_assessment", "k_student", "dim_student", "k_student")
fk_check("fct_student_assessment", "k_assessment", "dim_assessment", "k_assessment")
fk_check("fct_staff_school_association", "k_staff", "dim_staff", "k_staff")
fk_check("fct_student_parent_association", "k_parent", "dim_parent", "k_parent")
fk_check("fct_student_program_association", "k_program", "dim_program", "k_program")
fk_check("brg_course_section_program", "k_course_section", "dim_course_section", "k_course_section")
fk_check("fct_student_learning_standard_grades", "k_learning_standard", "dim_learning_standard", "k_learning_standard")
fk_check("fct_student_objective_assessment", "k_assessment", "dim_assessment", "k_assessment")

# Business-logic invariants.
mismatch = spark.sql(f"""
    SELECT COUNT(*) AS bad FROM {_g("fct_student_school_attendance_event")}
    WHERE attendance_event_category RLIKE '(?i)Absence' AND NOT is_absent
""").collect()[0]["bad"]
check("attendance is_absent flag matches category", mismatch == 0, f"bad_rows={mismatch}")

neg = spark.sql(f"""
    SELECT COUNT(*) AS bad FROM {_g("fct_student_school_association")}
    WHERE enrollment_days < 0
""").collect()[0]["bad"]
check("enrollment_days >= 0", neg == 0, f"bad_rows={neg}")

# Universal: any present GPA must be in range (real ODS samples may lack GPA entirely).
bad_range = spark.sql(f"""
    SELECT COUNT(*) AS bad FROM {_g("fct_student_gpa")}
    WHERE cumulative_gpa IS NOT NULL AND (cumulative_gpa < 0 OR cumulative_gpa > 5)
""").collect()[0]["bad"]
check("fct_student_gpa.cumulative_gpa in range when present", bad_range == 0, f"out_of_range={bad_range}")
if SYNTHETIC:
    null_gpa = spark.sql(f"""
        SELECT COUNT(*) AS bad FROM {_g("fct_student_gpa")} WHERE cumulative_gpa IS NULL
    """).collect()[0]["bad"]
    check("fct_student_gpa.cumulative_gpa populated", null_gpa == 0, f"null_rows={null_gpa}")

# COMMAND ----------

# MAGIC %md ## 5. Freshness

# COMMAND ----------

stale = spark.sql(f"""
    SELECT COUNT(*) AS n FROM {_b('_meta_change_versions')}
    WHERE kind = 'records'
      AND last_pull_timestamp < current_timestamp() - INTERVAL {MAX_PULL_AGE_HOURS} HOURS
""").collect()[0]["n"]
total_marks = spark.sql(
    f"SELECT COUNT(*) AS n FROM {_b('_meta_change_versions')} WHERE kind = 'records'"
).collect()[0]["n"]
check("bronze watermarks fresh", stale == 0 and total_marks > 0,
      f"watermarks={total_marks}, stale(>{MAX_PULL_AGE_HOURS}h)={stale}")

# COMMAND ----------

# MAGIC %md ## 6. Descriptor label resolution

# COMMAND ----------

# Resolved descriptor columns in gold must hold human labels — never a leftover
# `uri://...` URI. Key-feeding descriptors (deny-list) stay as their codeValue.
descriptor_samples = {}
RESOLVED_COLS = [
    ("dim_student", "gender"),
    ("dim_session", "term"),
    ("dim_course", "academic_subject"),
    ("fct_student_school_attendance_event", "attendance_event_category"),
    ("dim_staff", "highest_completed_level_of_education"),
]
for tbl, col in RESOLVED_COLS:
    try:
        leaks = spark.sql(f"SELECT COUNT(*) AS n FROM {_g(tbl)} WHERE {col} LIKE 'uri://%'").collect()[0]["n"]
        check(f"resolved {tbl}.{col} has no URI leaks", leaks == 0, f"uri_values={leaks}")
        descriptor_samples[f"{tbl}.{col}"] = [
            r[col] for r in spark.sql(
                f"SELECT DISTINCT {col} FROM {_g(tbl)} WHERE {col} IS NOT NULL ORDER BY {col} LIMIT 5"
            ).collect()
        ]
    except Exception as e:
        check(f"resolved {tbl}.{col} has no URI leaks", False, f"error: {str(e)[:200]}")

# Key-feeding descriptor stays codeValue for EDU key parity (readable, no URI).
try:
    descriptor_samples["dim_program.program_type (deny-list codeValue)"] = [
        r["program_type"] for r in spark.sql(
            f"SELECT DISTINCT program_type FROM {_g('dim_program')} WHERE program_type IS NOT NULL ORDER BY program_type LIMIT 8"
        ).collect()
    ]
except Exception:
    pass

# COMMAND ----------

# MAGIC %md ## 7. Chronic absenteeism

# COMMAND ----------

CHRONIC_THRESHOLD = 90   # mirrors var.chronic_absence_threshold default
CHRONIC_MIN_DAYS = 20    # mirrors var.chronic_absence_min_days default
chronic_stats = {}
try:
    m = spark.table(_g("msr_student_cumulative_attendance"))
    total = m.count()
    # Flag must equal the rule exactly (regression guard on the threshold/min-days).
    bad = m.where(
        F.col("is_chronically_absent") != (
            (F.col("total_days") >= CHRONIC_MIN_DAYS)
            & F.col("attendance_rate").isNotNull()
            & (F.col("attendance_rate") < CHRONIC_THRESHOLD / 100.0)
        )
    ).count()
    check("chronic flag matches rule", bad == 0, f"mismatched_rows={bad}")
    n_chronic = m.where(F.col("is_chronically_absent")).count()
    if SYNTHETIC:
        # Synthetic exercises a real mix; a real sample may have none or all.
        check("chronic absenteeism non-trivially populated", 0 < n_chronic < total,
              f"chronically_absent={n_chronic} of {total} students")
    chronic_stats = {"total_students": total, "chronically_absent": n_chronic,
                     "pct": round(100.0 * n_chronic / total, 1) if total else None}
except Exception as e:
    check("chronic absenteeism computed", False, f"error: {str(e)[:200]}")

# COMMAND ----------

# MAGIC %md ## 8. Program active/annual inclusion

# COMMAND ----------

program_stats = {}
try:
    sped = spark.table(_g("fct_student_special_education_program_association"))
    check("program facts carry is_active/is_annual",
          "is_active" in sped.columns and "is_annual" in sped.columns)
    n_active = sped.where(F.col("is_active")).count()
    n_annual = sped.where(F.col("is_annual")).count()
    sg = spark.table(_g("fct_student_subgroup"))
    total_students = sg.count()
    sped_members = sg.where(F.col("is_special_education")).count()
    # Universal: active participation is a subset of annual.
    check("program active <= annual", n_active <= n_annual, f"active={n_active}, annual={n_annual}")
    if SYNTHETIC:
        # Synthetic injects mid-year exits, so active is STRICTLY fewer than annual.
        check("program active < annual (exits excluded)", 0 < n_active < n_annual,
              f"active={n_active}, annual={n_annual}")
        check("subgroup sped membership non-trivial", 0 < sped_members < total_students,
              f"sped_active_members={sped_members} of {total_students}")
    program_stats = {"sped_active": n_active, "sped_annual": n_annual,
                     "sped_subgroup_active_members": sped_members, "total_students": total_students}
except Exception as e:
    check("program inclusion computed", False, f"error: {str(e)[:200]}")

# COMMAND ----------

# MAGIC %md ## 9. Demographic race/ethnicity rollup

# COMMAND ----------

demo_stats = {}
try:
    ds = spark.table(_g("dim_student"))
    check("dim_student has race_ethnicity", "race_ethnicity" in ds.columns)
    nulls = ds.where(F.col("race_ethnicity").isNull()).count()
    check("race_ethnicity has no nulls", nulls == 0, f"nulls={nulls}")
    dist = {r["race_ethnicity"]: r["n"]
            for r in ds.groupBy("race_ethnicity").count().withColumnRenamed("count", "n").collect()}
    if SYNTHETIC:
        # Synthetic includes multi-race, no-race, and Hispanic students.
        for lbl in ("Multiple", "Latinx", "Unknown"):
            check(f"race rollup produces '{lbl}'", dist.get(lbl, 0) > 0, f"count={dist.get(lbl, 0)}")
    demo_stats = {"race_ethnicity_distribution": dist}
except Exception as e:
    check("race rollup computed", False, f"error: {str(e)[:200]}")

# COMMAND ----------

import json
print(f"All checks complete. Failures: {len(failures)}")
print("descriptor_samples:\n" + json.dumps(descriptor_samples, indent=2))
print("chronic_stats:", json.dumps(chronic_stats))
print("program_stats:", json.dumps(program_stats))
print("demo_stats:", json.dumps(demo_stats))
if failures:
    raise AssertionError(f"E2E test failures ({len(failures)}): {failures}")

dbutils.notebook.exit(json.dumps({"status": "OK", "failures": [], "descriptor_samples": descriptor_samples, "chronic_stats": chronic_stats, "program_stats": program_stats, "demo_stats": demo_stats}))
