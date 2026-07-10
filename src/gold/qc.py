"""
EDU `models/qc/` equivalent — queryable data-quality summary tables.

These are gold tables prefixed `qc_*` that an analyst (or dashboard) can SELECT
from to answer:

  - Is bronze landing what we expect? (row counts per resource)
  - Are dimension PKs unique?               (qc_dim_pk_audit)
  - Are fact foreign keys resolving?         (qc_fct_fk_audit)
  - Are descriptors well-formed?             (qc_descriptor_audit)
  - When was each table last refreshed?      (qc_freshness)
  - Are business invariants holding?         (qc_business_invariants)

Pairs with inline SDP `@dlt.expect_*` annotations on individual tables.
Expectations show real-time DQ counts in the SDP UI; these tables let you
trend DQ over runs and build dashboards.
"""
from __future__ import annotations

import dlt
from pyspark.sql import DataFrame, SparkSession, functions as F


def _spark() -> SparkSession:
    s = SparkSession.getActiveSession()
    if s is None:
        s = SparkSession.builder.getOrCreate()
    return s


def _conf(key: str, default: str = "") -> str:
    return _spark().conf.get(key, default)


def _catalog() -> str:
    return _conf("edu.catalog", "edu_demo")


def _bronze_schema() -> str:
    return _conf("edu.bronze_schema", "edu_bronze")


def _silver_schema() -> str:
    return _conf("edu.silver_schema", "edu_silver")


def _gold_schema() -> str:
    return _conf("edu.gold_schema", "edu_gold")


def _tables_in(schema: str) -> list:
    df = _spark().sql(f"SHOW TABLES IN `{_catalog()}`.`{schema}`")
    return [r["tableName"] for r in df.collect()]


# ---------------------------------------------------------------------------
# 1. Row counts per table, per layer.  EDU dbt equivalent: qc/qc_row_counts.sql
# ---------------------------------------------------------------------------
@dlt.table(
    name="qc_row_counts",
    comment="Row count per table for each of bronze/silver/gold. Snapshot at pipeline run.",
    table_properties={"quality": "qc"},
)
def qc_row_counts():
    rows = []
    for schema, layer in (
        (_bronze_schema(), "bronze"),
        (_silver_schema(), "silver"),
        (_gold_schema(),   "gold"),
    ):
        for t in _tables_in(schema):
            # Skip the qc_* tables themselves to avoid self-reference loops.
            if t.startswith("qc_"):
                continue
            try:
                n = _spark().sql(f"SELECT COUNT(*) AS n FROM `{_catalog()}`.`{schema}`.`{t}`").collect()[0]["n"]
            except Exception:
                n = -1
            rows.append((_catalog(), schema, layer, t, int(n)))
    return _spark().createDataFrame(
        rows,
        "catalog STRING, schema STRING, layer STRING, table_name STRING, row_count BIGINT",
    ).withColumn("captured_at", F.current_timestamp())


# ---------------------------------------------------------------------------
# 2. Primary-key uniqueness audit for every dimension. EDU: qc/qc_dim_pk_audit.
# ---------------------------------------------------------------------------
_DIM_PKS = {
    "dim_lea": "k_lea",
    "dim_school": "k_school",
    "dim_session": "k_session",
    "dim_grading_period": "k_grading_period",
    "dim_calendar_date": "k_calendar_date",
    "dim_course": "k_course",
    "dim_course_section": "k_course_section",
    "dim_staff": "k_staff",
    "dim_student": "k_student",
    "dim_assessment": "k_assessment",
    "dim_esc": "k_esc",
    "dim_network": "k_network",
    "dim_program": "k_program",
    "dim_cohort": "k_cohort",
    "dim_parent": "k_parent",
    "dim_class_period": "k_class_period",
    "dim_classroom": "k_classroom",
    "dim_learning_standard": "k_learning_standard",
    "dim_objective_assessment": "k_objective_assessment",
    "dim_graduation_plan": "k_graduation_plan",
    "dim_school_calendar": "k_school_calendar",
    "dim_candidate": "k_candidate",
    "dim_educator_preparation_program": "k_educator_prep_program",
}


@dlt.table(
    name="qc_dim_pk_audit",
    comment="Per-dimension primary-key audit: row counts, distinct keys, null keys, duplicate count.",
    table_properties={"quality": "qc"},
)
def qc_dim_pk_audit():
    from pyspark.sql import functions as Ff
    # Use dlt.read() so SDP wires this as a downstream of every dim in the same pipeline.
    rows = []
    for dim, pk in _DIM_PKS.items():
        try:
            df = dlt.read(dim)
            agg = df.agg(
                Ff.count(Ff.lit(1)).alias("total_rows"),
                Ff.countDistinct(Ff.col(pk)).alias("distinct_keys"),
                Ff.sum(Ff.when(Ff.col(pk).isNull(), 1).otherwise(0)).alias("null_keys"),
            ).collect()[0]
            total = int(agg["total_rows"] or 0)
            distinct = int(agg["distinct_keys"] or 0)
            nulls = int(agg["null_keys"] or 0)
            dupes = total - distinct - nulls
            ok = (nulls == 0) and (dupes == 0)
            rows.append((dim, pk, total, distinct, nulls, dupes, ok))
        except Exception:
            rows.append((dim, pk, -1, -1, -1, -1, False))
    return _spark().createDataFrame(
        rows,
        "dim_table STRING, pk_column STRING, total_rows BIGINT, distinct_keys BIGINT, null_keys BIGINT, duplicate_count BIGINT, is_clean BOOLEAN",
    ).withColumn("captured_at", F.current_timestamp())


# ---------------------------------------------------------------------------
# 3. Foreign-key resolution audit. EDU: qc/qc_fct_fk_audit.
# ---------------------------------------------------------------------------
# Each entry: (fact_table, fk_column, dim_table, dim_pk_column)
_FACT_FKS = [
    ("fct_student_school_association", "k_student", "dim_student", "k_student"),
    ("fct_student_school_association", "k_school",  "dim_school",  "k_school"),
    ("fct_student_section_association", "k_student",        "dim_student",        "k_student"),
    ("fct_student_section_association", "k_course_section", "dim_course_section", "k_course_section"),
    ("fct_student_school_attendance_event", "k_student", "dim_student", "k_student"),
    ("fct_student_school_attendance_event", "k_school",  "dim_school",  "k_school"),
    ("fct_student_section_attendance_event", "k_student", "dim_student", "k_student"),
    ("fct_student_section_attendance_event", "k_course_section", "dim_course_section", "k_course_section"),
    ("fct_student_grades", "k_student",        "dim_student",        "k_student"),
    ("fct_student_grades", "k_course_section", "dim_course_section", "k_course_section"),
    ("fct_student_assessment", "k_student", "dim_student", "k_student"),
    ("fct_student_assessment", "k_assessment", "dim_assessment", "k_assessment"),
    ("fct_student_discipline_actions", "k_student", "dim_student", "k_student"),
    ("fct_student_discipline_actions", "k_school",  "dim_school",  "k_school"),
    ("fct_staff_school_association", "k_staff",  "dim_staff",  "k_staff"),
    ("fct_staff_school_association", "k_school", "dim_school", "k_school"),
    ("fct_staff_section_association", "k_staff", "dim_staff", "k_staff"),
    ("fct_staff_section_association", "k_course_section", "dim_course_section", "k_course_section"),
    ("fct_student_program_association", "k_student", "dim_student", "k_student"),
    ("fct_student_program_association", "k_program", "dim_program", "k_program"),
    ("fct_student_parent_association", "k_student", "dim_student", "k_student"),
    ("fct_student_parent_association", "k_parent",  "dim_parent",  "k_parent"),
    ("fct_student_cohort_association", "k_student", "dim_student", "k_student"),
    ("fct_student_cohort_association", "k_cohort",  "dim_cohort",  "k_cohort"),
    ("fct_course_transcripts", "k_student", "dim_student", "k_student"),
    ("fct_course_transcripts", "k_course",  "dim_course",  "k_course"),
    ("fct_student_learning_standard_grades", "k_student", "dim_student", "k_student"),
    ("fct_student_learning_standard_grades", "k_learning_standard", "dim_learning_standard", "k_learning_standard"),
    ("fct_student_objective_assessment", "k_student", "dim_student", "k_student"),
    ("fct_student_objective_assessment", "k_assessment", "dim_assessment", "k_assessment"),
    ("fct_candidate_educator_preparation_program_association", "k_candidate", "dim_candidate", "k_candidate"),
    ("fct_candidate_educator_preparation_program_association", "k_educator_prep_program", "dim_educator_preparation_program", "k_educator_prep_program"),
]


@dlt.table(
    name="qc_fct_fk_audit",
    comment="Foreign-key audit: counts of fact rows whose FK does not resolve to a dim row.",
    table_properties={"quality": "qc"},
)
def qc_fct_fk_audit():
    from pyspark.sql import functions as Ff
    rows = []
    for fact, fk, dim, dim_pk in _FACT_FKS:
        try:
            f_df = dlt.read(fact).select(Ff.col(fk).alias("fk")).where(Ff.col("fk").isNotNull())
            d_df = dlt.read(dim).select(Ff.col(dim_pk).alias("dk")).where(Ff.col("dk").isNotNull())
            fact_rows = f_df.count()
            orphans = (
                f_df.select("fk").distinct()
                    .join(d_df.select(Ff.col("dk").alias("fk")).distinct(), ["fk"], "left_anti")
                    .count()
            )
            rows.append((fact, fk, dim, dim_pk, fact_rows, orphans, orphans == 0))
        except Exception:
            rows.append((fact, fk, dim, dim_pk, -1, -1, False))
    return _spark().createDataFrame(
        rows,
        "fact_table STRING, fk_column STRING, dim_table STRING, dim_pk_column STRING, fact_rows BIGINT, orphan_keys BIGINT, is_clean BOOLEAN",
    ).withColumn("captured_at", F.current_timestamp())


# ---------------------------------------------------------------------------
# 4. Descriptor audit. Silver resolves most descriptor columns to human labels
#    (key-feeding descriptors stay as their codeValue URI). Flag any value that
#    is neither a clean non-empty label NOR a well-formed `uri://x#value`.
# ---------------------------------------------------------------------------
_DESCRIPTOR_COLUMNS = [
    # silver_table, descriptor_column — only true Ed-Fi descriptor URI fields
    ("students", "birth_sex_descriptor"),
    ("student_school_associations", "entry_grade_level_descriptor"),
    ("student_school_attendance_events", "attendance_event_category_descriptor"),
    ("grades", "grade_type_descriptor"),
    ("grades", "grading_period_descriptor"),
    ("staff_school_associations", "program_assignment_descriptor"),
    ("local_education_agencies", "lea_category_descriptor"),
    ("courses", "academic_subject_descriptor"),
]


@dlt.table(
    name="qc_descriptor_audit",
    comment="Per-descriptor-column audit: count of values that are empty or malformed (neither a clean label nor a well-formed descriptor URI).",
    table_properties={"quality": "qc"},
)
def qc_descriptor_audit():
    rows = []
    for tbl, col in _DESCRIPTOR_COLUMNS:
        try:
            r = _spark().sql(f"""
                SELECT
                    COUNT(*) AS total_rows,
                    SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS null_count,
                    SUM(CASE WHEN {col} IS NOT NULL AND (
                            trim({col}) = '' OR
                            ({col} LIKE '%://%' AND NOT {col} RLIKE 'uri://[^#]+#.+')
                        ) THEN 1 ELSE 0 END) AS malformed_count
                FROM `{_catalog()}`.`{_silver_schema()}`.`{tbl}`
            """).collect()[0]
            total = int(r["total_rows"])
            nulls = int(r["null_count"])
            malformed = int(r["malformed_count"])
            rows.append((tbl, col, total, nulls, malformed, malformed == 0))
        except Exception:
            rows.append((tbl, col, -1, -1, -1, False))
    return _spark().createDataFrame(
        rows,
        "silver_table STRING, descriptor_column STRING, total_rows BIGINT, null_count BIGINT, malformed_count BIGINT, is_clean BOOLEAN",
    ).withColumn("captured_at", F.current_timestamp())


# ---------------------------------------------------------------------------
# 5. Freshness — what does _meta_change_versions say about the latest pull?
# ---------------------------------------------------------------------------
@dlt.table(
    name="qc_freshness",
    comment="Latest pull-timestamp and change-version per Ed-Fi resource. EDU equivalent: change-version watermark report.",
    table_properties={"quality": "qc"},
)
def qc_freshness():
    return (
        _spark().read.table(f"`{_catalog()}`.`{_bronze_schema()}`.`_meta_change_versions`")
        .where(F.col("kind") == "records")
        .select(
            F.col("tenant_code"),
            F.col("api_year"),
            F.col("resource"),
            F.col("last_change_version"),
            F.col("last_pull_timestamp"),
            F.col("last_row_count"),
            F.when(
                F.col("last_pull_timestamp") >= F.current_timestamp() - F.expr("INTERVAL 26 HOURS"),
                F.lit("fresh"),
            ).otherwise(F.lit("stale")).alias("freshness_status"),
            F.current_timestamp().alias("captured_at"),
        )
    )


# ---------------------------------------------------------------------------
# 6. Business-invariant audit. Counts breaches of the rules our SDP
#    expectations also enforce — but as a queryable table you can dashboard.
# ---------------------------------------------------------------------------
@dlt.table(
    name="qc_business_invariants",
    comment="Counts of business-invariant breaches across gold facts.",
    table_properties={"quality": "qc"},
)
def qc_business_invariants():
    from pyspark.sql import functions as Ff
    rows = []

    def _count(name, df):
        try:
            n = int(df.count())
        except Exception:
            n = -1
        rows.append((name, n, n == 0))

    try:
        att = dlt.read("fct_student_school_attendance_event")
        bad = att.where(
            ((Ff.col("attendance_event_category").rlike("(?i)Absence")) & (~Ff.col("is_absent")))
            | ((~Ff.col("attendance_event_category").rlike("(?i)Absence")) & (Ff.col("is_absent") == True))
        )
        _count("attendance_is_absent_consistent", bad)
    except Exception:
        rows.append(("attendance_is_absent_consistent", -1, False))

    try:
        enr = dlt.read("fct_student_school_association")
        _count("enrollment_days_non_negative", enr.where(Ff.col("enrollment_days") < 0))
    except Exception:
        rows.append(("enrollment_days_non_negative", -1, False))

    try:
        g = dlt.read("fct_student_grades")
        bad = g.where(Ff.col("numeric_grade_earned").isNotNull() & ((Ff.col("numeric_grade_earned") < 0) | (Ff.col("numeric_grade_earned") > 100)))
        _count("numeric_grade_in_range", bad)
    except Exception:
        rows.append(("numeric_grade_in_range", -1, False))

    try:
        gpa = dlt.read("fct_student_gpa")
        bad = gpa.where(Ff.col("cumulative_gpa").isNotNull() & ((Ff.col("cumulative_gpa") < 0) | (Ff.col("cumulative_gpa") > 5)))
        _count("gpa_in_range", bad)
    except Exception:
        rows.append(("gpa_in_range", -1, False))

    try:
        d = dlt.read("fct_student_diploma").select("k_student", "diploma_award_date")
        e = dlt.read("fct_student_school_association").select("k_student", "entry_date")
        bad = d.join(e, "k_student").where(Ff.col("diploma_award_date") < Ff.col("entry_date"))
        _count("diploma_award_date_after_entry", bad)
    except Exception:
        rows.append(("diploma_award_date_after_entry", -1, False))

    # Descriptor lookup must exist and carry real labels — catches the
    # "hollow lookup" failure mode (resolution silently yields empty labels).
    try:
        desc = _spark().read.table(f"`{_catalog()}`.`{_silver_schema()}`.`_descriptors`")
        total = int(desc.count())
        hollow = int(
            desc.where(F.col("short_description").isNull() | (F.trim(F.col("short_description")) == "")).count()
        )
        rows.append(("descriptor_lookup_present", 0 if total > 0 else 1, total > 0))
        rows.append(("descriptor_lookup_non_hollow", hollow, hollow == 0))
    except Exception:
        rows.append(("descriptor_lookup_present", -1, False))
        rows.append(("descriptor_lookup_non_hollow", -1, False))

    return _spark().createDataFrame(
        rows,
        "invariant_name STRING, breach_count BIGINT, is_clean BOOLEAN",
    ).withColumn("captured_at", F.current_timestamp())


# ---------------------------------------------------------------------------
# 7. Overall scorecard: one row per QC dimension with a green/yellow/red status.
#    Makes it trivial to build a single-tile dashboard or feed a Genie space.
# ---------------------------------------------------------------------------
@dlt.table(
    name="qc_scorecard",
    comment="One-row-per-check scorecard summarizing all QC tables. Wire this to a dashboard.",
    table_properties={"quality": "qc"},
)
def qc_scorecard():
    pks = (
        dlt.read("qc_dim_pk_audit")
        .select(F.lit("dim_pk").alias("check_kind"), F.col("dim_table").alias("subject"),
                F.col("is_clean"))
    )
    fks = (
        dlt.read("qc_fct_fk_audit")
        .select(F.lit("fct_fk").alias("check_kind"),
                F.concat_ws(".", F.col("fact_table"), F.col("fk_column")).alias("subject"),
                F.col("is_clean"))
    )
    descriptors = (
        dlt.read("qc_descriptor_audit")
        .select(F.lit("descriptor").alias("check_kind"),
                F.concat_ws(".", F.col("silver_table"), F.col("descriptor_column")).alias("subject"),
                F.col("is_clean"))
    )
    invariants = (
        dlt.read("qc_business_invariants")
        .select(F.lit("invariant").alias("check_kind"),
                F.col("invariant_name").alias("subject"),
                F.col("is_clean"))
    )
    return (
        pks.unionByName(fks).unionByName(descriptors).unionByName(invariants)
        .withColumn(
            "status",
            F.when(F.col("is_clean"), F.lit("PASS")).otherwise(F.lit("FAIL")),
        )
        .withColumn("captured_at", F.current_timestamp())
    )
