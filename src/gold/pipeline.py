"""
Gold Spark Declarative Pipeline — the EDU `edu_wh` core_warehouse equivalent.

Builds dimensional and fact tables from the Silver staging layer. Each table
below names the EDU dbt model it replaces. Surrogate-key conventions match
edu_wh exactly (see gold/shared/keys.py), so joins compose the same way.

This pipeline implements the highest-value 17 marts from edu_wh. The full
EDU surface is ~57 core_warehouse models; see MART_PARITY.md for the
extension recipe to add the remaining marts.
"""
from __future__ import annotations

import dlt
from pyspark.sql import DataFrame, SparkSession, functions as F

from gold.shared.keys import (
    k_assessment, k_calendar_date, k_candidate, k_class_period, k_classroom, k_cohort,
    k_course, k_educator_prep_program, k_esc, k_graduation_plan, k_grading_period,
    k_learning_standard, k_lea, k_network, k_objective_assessment, k_parent, k_program,
    k_school, k_school_calendar, k_section, k_session, k_staff, k_student, k_student_xyear,
)
from silver.transforms import (
    extract_descriptor, chronic_absent_flag, program_window_flags, race_ethnicity_rollup,
)


def _spark() -> SparkSession:
    s = SparkSession.getActiveSession()
    if s is None:
        s = SparkSession.builder.getOrCreate()
    return s


def _conf(key: str, default: str = "") -> str:
    return _spark().conf.get(key, default)


def _silver(table: str) -> DataFrame:
    catalog = _conf("edu.catalog", "edu_demo")
    silver = _conf("edu.silver_schema", "edu_silver")
    return _spark().read.table(f"`{catalog}`.`{silver}`.`{table}`")


# ============================================================
# DIMENSIONS
# ============================================================

# SDP expectations applied to most gold tables. We use @dlt.expect (warn-only,
# counted in DQ metrics) so a single bad row doesn't fail the pipeline — that's
# the EDU dbt-test posture. Switch to @dlt.expect_or_fail for hard gates.
_DIM_EXPECTATIONS = {
    "tenant_code_not_null": "tenant_code IS NOT NULL",
    "school_year_not_null": "school_year IS NOT NULL",
}


@dlt.table(
    name="dim_lea",
    comment="LEA dimension. Replaces edu_wh.core_warehouse.dim_lea.",
    table_properties={"quality": "gold"},
)
@dlt.expect_all({**_DIM_EXPECTATIONS, "k_lea_not_null": "k_lea IS NOT NULL"})
def dim_lea():
    df = _silver("local_education_agencies")
    return df.select(
        k_lea("tenant_code", "api_year", "lea_id").alias("k_lea"),
        F.col("tenant_code"),
        F.col("api_year").alias("school_year"),
        F.col("lea_id"),
        F.col("name_of_institution").alias("lea_name"),
        extract_descriptor(F.col("lea_category_descriptor")).alias("lea_category"),
    )


@dlt.table(
    name="dim_school",
    comment="School dimension. Replaces edu_wh.core_warehouse.dim_school.",
    table_properties={"quality": "gold"},
)
@dlt.expect_all({**_DIM_EXPECTATIONS, "k_school_not_null": "k_school IS NOT NULL", "school_id_not_null": "school_id IS NOT NULL"})
def dim_school():
    s = _silver("schools")
    return s.select(
        k_school("tenant_code", "api_year", "school_id").alias("k_school"),
        k_lea("tenant_code", "api_year", "lea_id").alias("k_lea"),
        F.col("tenant_code"),
        F.col("api_year").alias("school_year"),
        F.col("school_id"),
        F.col("name_of_institution").alias("school_name"),
        F.col("lea_id"),
    )


@dlt.table(
    name="dim_session",
    comment="Session dimension. Replaces edu_wh.core_warehouse.dim_session.",
    table_properties={"quality": "gold"},
)
@dlt.expect_all({**_DIM_EXPECTATIONS, "k_session_not_null": "k_session IS NOT NULL"})
def dim_session():
    s = _silver("sessions")
    return s.select(
        k_session("tenant_code", "school_id", "school_year", "session_name").alias("k_session"),
        k_school("tenant_code", "api_year", "school_id").alias("k_school"),
        F.col("tenant_code"),
        F.col("school_id"),
        F.col("school_year"),
        F.col("session_name"),
        F.col("begin_date").cast("date").alias("begin_date"),
        F.col("end_date").cast("date").alias("end_date"),
        F.col("total_instructional_days").cast("int").alias("total_instructional_days"),
        extract_descriptor(F.col("term_descriptor")).alias("term"),
    )


@dlt.table(
    name="dim_grading_period",
    comment="Grading-period dimension. Replaces edu_wh.core_warehouse.dim_grading_period.",
    table_properties={"quality": "gold"},
)
def dim_grading_period():
    gp = _silver("grading_periods")
    return gp.select(
        k_grading_period(
            "tenant_code", "school_id", "school_year",
            F.coalesce(F.col("grading_period_name"), extract_descriptor(F.col("grading_period_descriptor"))),
        ).alias("k_grading_period"),
        k_school("tenant_code", "api_year", "school_id").alias("k_school"),
        F.col("tenant_code"),
        F.col("school_id"),
        F.col("school_year"),
        F.coalesce(F.col("grading_period_name"), extract_descriptor(F.col("grading_period_descriptor"))).alias("grading_period"),
        extract_descriptor(F.col("grading_period_descriptor")).alias("grading_period_type"),
        F.col("period_sequence").cast("int").alias("period_sequence"),
        F.col("begin_date").cast("date").alias("begin_date"),
        F.col("end_date").cast("date").alias("end_date"),
    )


@dlt.table(
    name="dim_calendar_date",
    comment="Calendar-date dimension. Replaces edu_wh.core_warehouse.dim_calendar_date.",
    table_properties={"quality": "gold"},
)
def dim_calendar_date():
    cd = _silver("calendar_dates")
    return cd.select(
        k_calendar_date("tenant_code", "school_id", "school_year", "calendar_code", "calendar_date").alias("k_calendar_date"),
        k_school("tenant_code", "api_year", "school_id").alias("k_school"),
        F.col("tenant_code"),
        F.col("school_id"),
        F.col("school_year"),
        F.col("calendar_code"),
        F.col("calendar_date").cast("date").alias("calendar_date"),
        F.dayofweek(F.col("calendar_date").cast("date")).alias("day_of_week"),
        F.weekofyear(F.col("calendar_date").cast("date")).alias("week_of_year"),
        F.month(F.col("calendar_date").cast("date")).alias("month"),
        F.year(F.col("calendar_date").cast("date")).alias("year"),
    )


@dlt.table(
    name="dim_course",
    comment="Course dimension. Replaces edu_wh.core_warehouse.dim_course.",
    table_properties={"quality": "gold"},
)
def dim_course():
    c = _silver("courses")
    return c.select(
        k_course("tenant_code", "api_year", "course_code", "education_organization_id").alias("k_course"),
        F.col("tenant_code"),
        F.col("api_year").alias("school_year"),
        F.col("course_code"),
        F.col("education_organization_id"),
        F.col("course_title"),
        extract_descriptor(F.col("academic_subject_descriptor")).alias("academic_subject"),
        F.col("number_of_parts").cast("int").alias("number_of_parts"),
    )


@dlt.table(
    name="dim_course_section",
    comment="Course section dimension. Replaces edu_wh.core_warehouse.dim_course_section.",
    table_properties={"quality": "gold"},
)
@dlt.expect_all({**_DIM_EXPECTATIONS, "k_course_section_not_null": "k_course_section IS NOT NULL"})
def dim_course_section():
    s = _silver("sections")
    co = _silver("course_offerings").select(
        "tenant_code", "school_id", "school_year", "session_name", "local_course_code",
        "course_code", "course_education_organization_id", "local_course_title",
    )
    return (
        s.join(co, on=["tenant_code", "school_id", "school_year", "session_name", "local_course_code"], how="left")
         .select(
            k_section("tenant_code", "school_id", "school_year", "session_name",
                       "local_course_code", "section_identifier").alias("k_course_section"),
            k_school("tenant_code", "api_year", "school_id").alias("k_school"),
            k_session("tenant_code", "school_id", "school_year", "session_name").alias("k_session"),
            F.col("tenant_code"),
            F.col("school_id"),
            F.col("school_year"),
            F.col("session_name"),
            F.col("local_course_code"),
            F.col("section_identifier"),
            F.col("local_course_title"),
            F.col("course_code"),
            F.col("course_education_organization_id"),
            extract_descriptor(F.col("educational_environment_descriptor")).alias("educational_environment"),
            F.col("available_credits").cast("double").alias("available_credits"),
        )
    )


@dlt.table(
    name="dim_staff",
    comment="Staff dimension. Replaces edu_wh.core_warehouse.dim_staff.",
    table_properties={"quality": "gold"},
)
@dlt.expect_all({**_DIM_EXPECTATIONS, "k_staff_not_null": "k_staff IS NOT NULL", "staff_unique_id_not_null": "staff_unique_id IS NOT NULL"})
def dim_staff():
    s = _silver("staffs")
    return s.select(
        k_staff("tenant_code", "api_year", "staff_unique_id").alias("k_staff"),
        F.col("tenant_code"),
        F.col("api_year").alias("school_year"),
        F.col("staff_unique_id"),
        F.col("first_name"),
        F.col("last_name"),
        F.concat_ws(" ", "first_name", "last_name").alias("display_name"),
        F.col("birth_date").cast("date").alias("birth_date"),
        extract_descriptor(F.col("highest_completed_level_of_education_descriptor")).alias("highest_completed_level_of_education"),
    )


@dlt.table(
    name="dim_student",
    comment="Student dimension. Replaces edu_wh.core_warehouse.dim_student.",
    table_properties={"quality": "gold"},
)
@dlt.expect_all({
    **_DIM_EXPECTATIONS,
    "k_student_not_null": "k_student IS NOT NULL",
    "student_unique_id_not_null": "student_unique_id IS NOT NULL",
})
def dim_student():
    stu = _silver("students")
    from pyspark.sql.window import Window
    # Students carry one edorg association per LEA AND school on real ODSes.
    # Keep exactly one demographics row per student: the most complete one
    # (EDU enforces this same uniqueness on its demographics source).
    eo = (
        _silver("student_education_organization_associations")
        .select(
            "tenant_code", "api_year", "student_unique_id",
            "education_organization_id",
            extract_descriptor(F.col("sex_descriptor")).alias("gender"),
            extract_descriptor(F.col("limited_english_proficiency_descriptor")).alias("lep_code"),
            F.col("hispanic_latino_ethnicity").cast("boolean").alias("has_hispanic_latino_ethnicity"),
            F.col("races_json"),
        )
        .withColumn(
            "_completeness",
            F.col("gender").isNotNull().cast("int")
            + F.col("lep_code").isNotNull().cast("int")
            + F.col("has_hispanic_latino_ethnicity").isNotNull().cast("int")
            + F.col("races_json").isNotNull().cast("int"),
        )
        .withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy("tenant_code", "api_year", "student_unique_id")
                      .orderBy(F.col("_completeness").desc(), F.col("education_organization_id").desc())
            ),
        )
        .filter("_rn = 1")
        .drop("_rn", "_completeness", "education_organization_id")
    )
    # Federal race/ethnicity rollup from races[] + Hispanic flag (labels tunable).
    _race_codes = F.array_distinct(F.expr(
        "transform(coalesce(from_json(races_json, 'array<struct<raceDescriptor:string>>'), array()),"
        " x -> element_at(split(x.raceDescriptor, '#'), -1))"
    ))
    eo = eo.withColumn(
        "race_ethnicity",
        race_ethnicity_rollup(
            _race_codes, F.col("has_hispanic_latino_ethnicity"),
            _conf("edu.multiple_races_code", "Multiple"),
            _conf("edu.hispanic_latino_code", "Latinx"),
            _conf("edu.race_unknown_code", "Unknown"),
        ),
    ).drop("races_json")
    ssa = _silver("student_school_associations").select(
        "tenant_code", "api_year", "student_unique_id", "school_id",
        extract_descriptor(F.col("entry_grade_level_descriptor")).alias("grade_level"),
        F.col("entry_date").cast("date").alias("entry_date"),
    )
    # most-recent school per student
    from pyspark.sql.window import Window
    latest_ssa = ssa.withColumn(
        "_rn", F.row_number().over(Window.partitionBy("tenant_code", "api_year", "student_unique_id").orderBy(F.col("entry_date").desc()))
    ).filter("_rn = 1").drop("_rn")

    return (
        stu
        .join(eo, on=["tenant_code", "api_year", "student_unique_id"], how="left")
        .join(latest_ssa, on=["tenant_code", "api_year", "student_unique_id"], how="left")
        .select(
            k_student("tenant_code", "api_year", "student_unique_id").alias("k_student"),
            k_student_xyear("tenant_code", "student_unique_id").alias("k_student_xyear"),
            F.col("tenant_code"),
            F.col("api_year").alias("school_year"),
            F.col("student_unique_id"),
            F.col("first_name"),
            F.col("middle_name"),
            F.col("last_name"),
            F.concat_ws(" ", "first_name", "last_name").alias("display_name"),
            F.col("birth_date").cast("date").alias("birth_date"),
            F.col("birth_city"),
            extract_descriptor(F.col("birth_sex_descriptor")).alias("birth_sex"),
            F.col("gender"),
            F.col("lep_code"),
            F.col("has_hispanic_latino_ethnicity"),
            # Students with no education-org association (left-join miss) still get
            # a value — never NULL — so the rollup is total over every student.
            F.coalesce(F.col("race_ethnicity"), F.lit(_conf("edu.race_unknown_code", "Unknown"))).alias("race_ethnicity"),
            F.col("grade_level"),
            F.col("school_id"),
            F.col("entry_date"),
        )
    )


@dlt.table(
    name="dim_assessment",
    comment="Assessment dimension. Replaces edu_wh.core_warehouse.dim_assessment.",
    table_properties={"quality": "gold"},
)
def dim_assessment():
    a = _silver("assessments")
    return a.select(
        k_assessment("tenant_code", "assessment_identifier", "namespace").alias("k_assessment"),
        F.col("tenant_code"),
        F.col("api_year").alias("school_year"),
        F.col("assessment_identifier"),
        F.col("namespace"),
        F.col("assessment_title"),
        extract_descriptor(F.col("assessment_category_descriptor")).alias("assessment_category"),
    )


# ============================================================
# FACTS
# ============================================================

@dlt.table(
    name="fct_student_school_association",
    comment="Student enrollment fact (one row per student-school-entry-date). Replaces edu_wh.core_warehouse.fct_student_school_association.",
    table_properties={"quality": "gold"},
)
@dlt.expect_all({
    "k_student_not_null": "k_student IS NOT NULL",
    "k_school_not_null":  "k_school IS NOT NULL",
    "entry_date_not_null": "entry_date IS NOT NULL",
    "enrollment_days_non_negative": "enrollment_days IS NULL OR enrollment_days >= 0",
})
def fct_student_school_association():
    ssa = _silver("student_school_associations")
    return ssa.select(
        k_student("tenant_code", "api_year", "student_unique_id").alias("k_student"),
        k_school("tenant_code", "api_year", "school_id").alias("k_school"),
        F.col("tenant_code"),
        F.coalesce(F.col("school_year"), F.col("api_year")).alias("school_year"),
        F.col("student_unique_id"),
        F.col("school_id"),
        F.col("entry_date").cast("date").alias("entry_date"),
        F.col("exit_withdraw_date").cast("date").alias("exit_withdraw_date"),
        extract_descriptor(F.col("entry_grade_level_descriptor")).alias("entry_grade_level"),
        F.datediff(
            F.coalesce(F.col("exit_withdraw_date").cast("date"), F.current_date()),
            F.col("entry_date").cast("date"),
        ).alias("enrollment_days"),
    )


@dlt.table(
    name="fct_student_section_association",
    comment="Student-section enrollment fact. Replaces edu_wh.core_warehouse.fct_student_section_association.",
    table_properties={"quality": "gold"},
)
@dlt.expect_all({
    "k_student_not_null": "k_student IS NOT NULL",
    "k_course_section_not_null": "k_course_section IS NOT NULL",
})
def fct_student_section_association():
    ssa = _silver("student_section_associations")
    return ssa.select(
        k_student("tenant_code", "api_year", "student_unique_id").alias("k_student"),
        k_section("tenant_code", "school_id", "school_year", "session_name",
                   "local_course_code", "section_identifier").alias("k_course_section"),
        k_school("tenant_code", "api_year", "school_id").alias("k_school"),
        k_session("tenant_code", "school_id", "school_year", "session_name").alias("k_session"),
        F.col("tenant_code"),
        F.col("school_year"),
        F.col("student_unique_id"),
        F.col("school_id"),
        F.col("session_name"),
        F.col("local_course_code"),
        F.col("section_identifier"),
        F.col("begin_date").cast("date").alias("begin_date"),
        F.col("end_date").cast("date").alias("end_date"),
    )


@dlt.table(
    name="fct_student_school_attendance_event",
    comment="Daily school-level attendance event fact. Replaces edu_wh.core_warehouse.fct_student_school_attendance_event.",
    table_properties={"quality": "gold"},
)
@dlt.expect_all({
    "k_student_not_null": "k_student IS NOT NULL",
    "k_school_not_null":  "k_school IS NOT NULL",
    "attendance_event_date_not_null": "attendance_event_date IS NOT NULL",
    "is_absent_consistent_with_category": "(attendance_event_category RLIKE '(?i)Absence' AND is_absent = TRUE) OR (NOT (attendance_event_category RLIKE '(?i)Absence') AND is_absent = FALSE)",
})
def fct_student_school_attendance_event():
    ev = _silver("student_school_attendance_events")
    return ev.select(
        k_student("tenant_code", "api_year", "student_unique_id").alias("k_student"),
        k_school("tenant_code", "api_year", "school_id").alias("k_school"),
        k_session("tenant_code", "school_id", "school_year", "session_name").alias("k_session"),
        F.col("tenant_code"),
        F.col("school_year"),
        F.col("student_unique_id"),
        F.col("school_id"),
        F.col("session_name"),
        F.col("attendance_event_date").cast("date").alias("attendance_event_date"),
        extract_descriptor(F.col("attendance_event_category_descriptor")).alias("attendance_event_category"),
        F.col("attendance_event_reason"),
        F.when(
            extract_descriptor(F.col("attendance_event_category_descriptor")).rlike("(?i)Absence"),
            F.lit(True),
        ).otherwise(F.lit(False)).alias("is_absent"),
        F.col("event_duration").cast("double").alias("event_duration"),
    )


@dlt.table(
    name="fct_student_grades",
    comment="Student grades fact (one row per student-section-grading-period). Replaces edu_wh.core_warehouse.fct_student_grades.",
    table_properties={"quality": "gold"},
)
@dlt.expect_all({
    "k_student_not_null": "k_student IS NOT NULL",
    "k_course_section_not_null": "k_course_section IS NOT NULL",
    "numeric_grade_in_range": "numeric_grade_earned IS NULL OR (numeric_grade_earned >= 0 AND numeric_grade_earned <= 100)",
})
def fct_student_grades():
    g = _silver("grades")
    return g.select(
        k_student("tenant_code", "api_year", "student_unique_id").alias("k_student"),
        k_section("tenant_code", "school_id", "school_year", "session_name",
                   "local_course_code", "section_identifier").alias("k_course_section"),
        k_grading_period(
            "tenant_code", "school_id", "school_year",
            F.coalesce(F.col("grading_period_name"), extract_descriptor(F.col("grading_period_descriptor"))),
        ).alias("k_grading_period"),
        F.col("tenant_code"),
        F.col("school_year"),
        F.col("student_unique_id"),
        F.col("school_id"),
        F.col("session_name"),
        F.col("local_course_code"),
        F.col("section_identifier"),
        F.coalesce(F.col("grading_period_name"), extract_descriptor(F.col("grading_period_descriptor"))).alias("grading_period"),
        extract_descriptor(F.col("grade_type_descriptor")).alias("grade_type"),
        F.col("numeric_grade_earned").cast("double").alias("numeric_grade_earned"),
        F.col("letter_grade_earned"),
    )


@dlt.table(
    name="fct_student_assessment",
    comment="Student assessment fact. Replaces edu_wh.core_warehouse.fct_student_assessment.",
    table_properties={"quality": "gold"},
)
@dlt.expect_all({
    "k_student_not_null": "k_student IS NOT NULL",
    "k_assessment_not_null": "k_assessment IS NOT NULL",
})
def fct_student_assessment():
    sa = _silver("student_assessments")
    return sa.select(
        k_student("tenant_code", "api_year", "student_unique_id").alias("k_student"),
        k_assessment("tenant_code", "assessment_identifier", "namespace").alias("k_assessment"),
        F.col("tenant_code"),
        F.col("api_year").alias("school_year"),
        F.col("student_unique_id"),
        F.col("student_assessment_identifier"),
        F.col("assessment_identifier"),
        F.col("namespace"),
        F.col("administration_date").cast("date").alias("administration_date"),
    )


@dlt.table(
    name="fct_student_discipline_actions",
    comment="Discipline action fact. Replaces edu_wh.core_warehouse.fct_student_discipline_actions.",
    table_properties={"quality": "gold"},
)
def fct_student_discipline_actions():
    da = _silver("discipline_actions")
    # DS 5.x: no plain schoolReference — the responsibility school owns the
    # action; assignment school kept as a detail column.
    school_id = F.coalesce(F.col("responsibility_school_id"), F.col("assignment_school_id"))
    return da.select(
        k_student("tenant_code", "api_year", "student_unique_id").alias("k_student"),
        k_school("tenant_code", "api_year", school_id).alias("k_school"),
        F.col("tenant_code"),
        F.col("api_year").alias("school_year"),
        F.col("student_unique_id"),
        school_id.alias("school_id"),
        F.col("assignment_school_id"),
        F.col("discipline_action_identifier"),
        F.col("discipline_date").cast("date").alias("discipline_date"),
    )


@dlt.table(
    name="fct_staff_school_association",
    comment="Staff-school assignment fact. Replaces edu_wh.core_warehouse.fct_staff_school_association.",
    table_properties={"quality": "gold"},
)
def fct_staff_school_association():
    ssa = _silver("staff_school_associations")
    return ssa.select(
        k_staff("tenant_code", "api_year", "staff_unique_id").alias("k_staff"),
        k_school("tenant_code", "api_year", "school_id").alias("k_school"),
        F.col("tenant_code"),
        F.col("api_year").alias("school_year"),
        F.col("staff_unique_id"),
        F.col("school_id"),
        extract_descriptor(F.col("program_assignment_descriptor")).alias("program_assignment"),
    )


# ============================================================
# ADDITIONAL DIMENSIONS — full EDU core_warehouse parity batch
# ============================================================

@dlt.table(name="dim_esc", comment="Education Service Center dim. Replaces edu_wh.dim_esc.",
           table_properties={"quality": "gold"})
def dim_esc():
    e = _silver("education_service_centers")
    return e.select(
        k_esc("tenant_code", "api_year", "education_service_center_id").alias("k_esc"),
        F.col("tenant_code"), F.col("api_year").alias("school_year"),
        F.col("education_service_center_id"),
        F.col("name_of_institution").alias("esc_name"),
    )


@dlt.table(name="dim_network", comment="Ed-org network dim. Replaces edu_wh.dim_network.",
           table_properties={"quality": "gold"})
def dim_network():
    n = _silver("education_organization_networks")
    return n.select(
        k_network("tenant_code", "api_year", "network_id").alias("k_network"),
        F.col("tenant_code"), F.col("api_year").alias("school_year"),
        F.col("network_id"),
        F.col("network_name"),
        extract_descriptor(F.col("network_purpose_descriptor")).alias("network_purpose"),
    )


@dlt.table(name="dim_program", comment="Program dim. Replaces edu_wh.dim_program.",
           table_properties={"quality": "gold"})
def dim_program():
    p = _silver("programs")
    return p.select(
        k_program(
            "tenant_code", "api_year", "program_name",
            extract_descriptor(F.col("program_type_descriptor")),
            "education_organization_id",
        ).alias("k_program"),
        F.col("tenant_code"), F.col("api_year").alias("school_year"),
        F.col("program_name"),
        extract_descriptor(F.col("program_type_descriptor")).alias("program_type"),
        F.col("education_organization_id"),
    )


@dlt.table(name="dim_cohort", comment="Cohort dim. Replaces edu_wh.dim_cohort.",
           table_properties={"quality": "gold"})
def dim_cohort():
    c = _silver("cohorts")
    return c.select(
        k_cohort("tenant_code", "api_year", "cohort_identifier", "education_organization_id").alias("k_cohort"),
        F.col("tenant_code"), F.col("api_year").alias("school_year"),
        F.col("cohort_identifier"),
        F.col("education_organization_id"),
        F.col("cohort_description"),
        extract_descriptor(F.col("cohort_type_descriptor")).alias("cohort_type"),
    )


@dlt.table(name="dim_parent", comment="Parent/contact dim. Replaces edu_wh.dim_parent (incl. its parent/contact bridge: 'parents' < DS 5.x, 'contacts' >= DS 5.x).",
           table_properties={"quality": "gold"})
def dim_parent():
    def _shape(df, id_col, source):
        return df.select(
            k_parent("tenant_code", "api_year", id_col).alias("k_parent"),
            F.col("tenant_code"), F.col("api_year").alias("school_year"),
            F.col(id_col).alias("parent_unique_id"),
            F.col("first_name"),
            F.col("last_name"),
            F.concat_ws(" ", "first_name", "last_name").alias("display_name"),
            extract_descriptor(F.col("sex_descriptor")).alias("gender"),
            F.lit(source).alias("source_endpoint"),
        )
    return _shape(_silver("parents"), "parent_unique_id", "parents").unionByName(
        _shape(_silver("contacts"), "contact_unique_id", "contacts")
    )


@dlt.table(name="dim_class_period", comment="Class-period dim. Replaces edu_wh.dim_class_period.",
           table_properties={"quality": "gold"})
def dim_class_period():
    cp = _silver("class_periods")
    return cp.select(
        k_class_period("tenant_code", "api_year", "class_period_name", "school_id").alias("k_class_period"),
        F.col("tenant_code"), F.col("api_year").alias("school_year"),
        F.col("class_period_name"),
        F.col("school_id"),
    )


@dlt.table(name="dim_classroom", comment="Classroom/location dim. Replaces edu_wh.dim_classroom.",
           table_properties={"quality": "gold"})
def dim_classroom():
    loc = _silver("locations")
    return loc.select(
        k_classroom("tenant_code", "api_year", "classroom_identification_code", "school_id").alias("k_classroom"),
        F.col("tenant_code"), F.col("api_year").alias("school_year"),
        F.col("classroom_identification_code"),
        F.col("school_id"),
        F.col("maximum_number_of_seats").cast("int").alias("maximum_number_of_seats"),
    )


@dlt.table(name="dim_learning_standard", comment="Learning standard dim. Replaces edu_wh.dim_learning_standard.",
           table_properties={"quality": "gold"})
def dim_learning_standard():
    ls = _silver("learning_standards")
    return ls.select(
        k_learning_standard("tenant_code", "learning_standard_id").alias("k_learning_standard"),
        F.col("tenant_code"), F.col("api_year").alias("school_year"),
        F.col("learning_standard_id"),
        F.col("description"),
        extract_descriptor(F.col("academic_subject_descriptor")).alias("academic_subject"),
    )


@dlt.table(name="dim_objective_assessment", comment="Objective assessment dim. Replaces edu_wh.dim_objective_assessment.",
           table_properties={"quality": "gold"})
def dim_objective_assessment():
    o = _silver("objective_assessments")
    return o.select(
        k_objective_assessment("tenant_code", "assessment_identifier", "identification_code", "namespace").alias("k_objective_assessment"),
        k_assessment("tenant_code", "assessment_identifier", "namespace").alias("k_assessment"),
        F.col("tenant_code"), F.col("api_year").alias("school_year"),
        F.col("assessment_identifier"),
        F.col("identification_code"),
        F.col("namespace"),
        F.col("description"),
    )


@dlt.table(name="dim_graduation_plan", comment="Graduation plan dim. Replaces edu_wh.dim_graduation_plan.",
           table_properties={"quality": "gold"})
def dim_graduation_plan():
    g = _silver("graduation_plans")
    return g.select(
        k_graduation_plan(
            "tenant_code", "education_organization_id",
            extract_descriptor(F.col("graduation_plan_type_descriptor")),
            "graduation_school_year",
        ).alias("k_graduation_plan"),
        F.col("tenant_code"), F.col("api_year").alias("school_year"),
        F.col("education_organization_id"),
        extract_descriptor(F.col("graduation_plan_type_descriptor")).alias("graduation_plan_type"),
        F.col("graduation_school_year").cast("int").alias("graduation_school_year"),
        F.col("total_required_credits").cast("double").alias("total_required_credits"),
    )


@dlt.table(name="dim_school_calendar", comment="School-calendar dim. Replaces edu_wh.dim_school_calendar.",
           table_properties={"quality": "gold"})
def dim_school_calendar():
    cal = _silver("calendars")
    return cal.select(
        k_school_calendar("tenant_code", "school_id", "school_year", "calendar_code").alias("k_school_calendar"),
        k_school("tenant_code", "api_year", "school_id").alias("k_school"),
        F.col("tenant_code"), F.col("school_year"),
        F.col("calendar_code"),
        F.col("school_id"),
        extract_descriptor(F.col("calendar_type_descriptor")).alias("calendar_type"),
    )


# ============================================================
# ADDITIONAL FACTS — full EDU core_warehouse parity batch
# ============================================================

@dlt.table(name="fct_student_program_association",
           comment="Generic student-program association fact. Replaces edu_wh.fct_student_program_association.",
           table_properties={"quality": "gold"})
def fct_student_program_association():
    p = _silver("student_program_associations")
    return p.select(
        k_student("tenant_code", "api_year", "student_unique_id").alias("k_student"),
        k_program(
            "tenant_code", "api_year", "program_name",
            extract_descriptor(F.col("program_type_descriptor")),
            "education_organization_id",
        ).alias("k_program"),
        F.col("tenant_code"), F.col("api_year").alias("school_year"),
        F.col("student_unique_id"),
        F.col("education_organization_id"),
        F.col("program_name"),
        extract_descriptor(F.col("program_type_descriptor")).alias("program_type"),
        F.col("begin_date").cast("date").alias("begin_date"),
        F.col("end_date").cast("date").alias("end_date"),
    )


def _program_assoc_select(p):
    # Keep every association row, but flag annual (served this year) vs active
    # (still open at year-end) so downstream counts aren't inflated by exits.
    has_end = "end_date" in p.columns
    begin = F.col("begin_date").cast("date")
    end = (F.col("end_date").cast("date") if has_end else F.lit(None).cast("date"))
    is_annual, is_active = program_window_flags(begin, end, F.col("api_year"))
    return p.select(
        k_student("tenant_code", "api_year", "student_unique_id").alias("k_student"),
        F.col("tenant_code"), F.col("api_year").alias("school_year"),
        F.col("student_unique_id"),
        F.col("education_organization_id"),
        F.col("program_name"),
        begin.alias("begin_date"),
        end.alias("end_date"),
        is_annual.alias("is_annual"),
        is_active.alias("is_active"),
    )


@dlt.table(name="fct_student_special_education_program_association",
           comment="Special-ed program association. Replaces edu_wh.fct_student_special_education_program_association.",
           table_properties={"quality": "gold"})
def fct_student_special_education_program_association():
    return _program_assoc_select(_silver("student_special_education_program_associations"))


@dlt.table(name="fct_student_homeless_program_association",
           comment="Homeless program association. Replaces edu_wh.fct_student_homeless_program_association.",
           table_properties={"quality": "gold"})
def fct_student_homeless_program_association():
    return _program_assoc_select(_silver("student_homeless_program_associations"))


@dlt.table(name="fct_student_language_instruction_program_association",
           comment="Language-instruction program association. Replaces edu_wh.fct_student_language_instruction_program_association.",
           table_properties={"quality": "gold"})
def fct_student_language_instruction_program_association():
    return _program_assoc_select(_silver("student_language_instruction_program_associations"))


@dlt.table(name="fct_student_title_i_part_a_program_association",
           comment="Title I-A program association. Replaces edu_wh.fct_student_title_i_part_a_program_association.",
           table_properties={"quality": "gold"})
def fct_student_title_i_part_a_program_association():
    return _program_assoc_select(_silver("student_title_i_part_a_program_associations"))


@dlt.table(name="fct_student_cte_program_associations",
           comment="CTE program association. Replaces edu_wh.fct_student_cte_program_associations.",
           table_properties={"quality": "gold"})
def fct_student_cte_program_associations():
    return _program_assoc_select(_silver("student_cte_program_associations"))


@dlt.table(name="fct_student_migrant_education_program_associations",
           comment="Migrant education program association. Replaces edu_wh.fct_student_migrant_education_program_associations.",
           table_properties={"quality": "gold"})
def fct_student_migrant_education_program_associations():
    return _program_assoc_select(_silver("student_migrant_education_program_associations"))


@dlt.table(name="fct_student_school_food_service_program_associations",
           comment="Food-service program association. Replaces edu_wh.fct_student_school_food_service_program_associations.",
           table_properties={"quality": "gold"})
def fct_student_school_food_service_program_associations():
    return _program_assoc_select(_silver("student_school_food_service_program_association"))


@dlt.table(name="fct_student_parent_association",
           comment="Student-parent/contact association fact. Replaces edu_wh.fct_student_parent_association (bridged across the DS 5.x parents->contacts rename).",
           table_properties={"quality": "gold"})
def fct_student_parent_association():
    def _shape(df, id_col):
        return df.select(
            k_student("tenant_code", "api_year", "student_unique_id").alias("k_student"),
            k_parent("tenant_code", "api_year", id_col).alias("k_parent"),
            F.col("tenant_code"), F.col("api_year").alias("school_year"),
            F.col("student_unique_id"),
            F.col(id_col).alias("parent_unique_id"),
            extract_descriptor(F.col("relation_descriptor")).alias("relation"),
            F.col("primary_contact_status").cast("boolean").alias("primary_contact_status"),
        )
    return _shape(_silver("student_parent_associations"), "parent_unique_id").unionByName(
        _shape(_silver("student_contact_associations"), "contact_unique_id")
    )


@dlt.table(name="fct_student_cohort_association",
           comment="Student-cohort association fact. Replaces edu_wh.fct_student_cohort_association.",
           table_properties={"quality": "gold"})
def fct_student_cohort_association():
    c = _silver("student_cohort_associations")
    return c.select(
        k_student("tenant_code", "api_year", "student_unique_id").alias("k_student"),
        k_cohort("tenant_code", "api_year", "cohort_identifier", "education_organization_id").alias("k_cohort"),
        F.col("tenant_code"), F.col("api_year").alias("school_year"),
        F.col("student_unique_id"),
        F.col("cohort_identifier"),
        F.col("education_organization_id"),
        F.col("begin_date").cast("date").alias("begin_date"),
        F.col("end_date").cast("date").alias("end_date"),
    )


def _gpa_columns(df):
    """DS 5.x: GPA lives in the gradePointAverages[] array; pre-5.x ODSes carry
    flat cumulative/session fields. Extract both shapes, prefer the array."""
    from pyspark.sql.types import ArrayType, StructType, StructField, StringType, DoubleType, BooleanType
    gpa_schema = ArrayType(StructType([
        StructField("gradePointAverageTypeDescriptor", StringType()),
        StructField("isCumulative", BooleanType()),
        StructField("gradePointAverageValue", DoubleType()),
        StructField("maxGradePointAverageValue", DoubleType()),
    ]))
    df = df.withColumn("_gpas", F.from_json(F.get_json_object(F.col("payload"), "$.gradePointAverages"), gpa_schema))
    cum_arr = F.filter(F.col("_gpas"), lambda g: g["isCumulative"] == True)  # noqa: E712
    ses_arr = F.filter(F.col("_gpas"), lambda g: g["isCumulative"] == False)  # noqa: E712
    return (
        df.withColumn(
            "cumulative_gpa",
            F.coalesce(
                F.element_at(cum_arr, 1).getField("gradePointAverageValue"),
                F.get_json_object(F.col("payload"), "$.cumulativeGradePointAverage").cast("double"),
            ),
        )
        .withColumn(
            "session_gpa",
            F.coalesce(
                F.element_at(ses_arr, 1).getField("gradePointAverageValue"),
                F.get_json_object(F.col("payload"), "$.sessionGradePointAverage").cast("double"),
            ),
        )
        .drop("_gpas")
    )


def _term_order(term_col):
    """Academic-term ordering (EDU: xwalk_academic_terms seed)."""
    return (
        F.when(term_col.rlike("(?i)fall"), 1)
         .when(term_col.rlike("(?i)winter"), 2)
         .when(term_col.rlike("(?i)spring"), 3)
         .when(term_col.rlike("(?i)summer"), 4)
         .otherwise(0)
    )


@dlt.table(name="fct_student_academic_record",
           comment="Academic record fact (term-level GPA/credits). Replaces edu_wh.fct_student_academic_record.",
           table_properties={"quality": "gold"})
def fct_student_academic_record():
    ar = _gpa_columns(_silver("student_academic_records"))
    return ar.select(
        k_student("tenant_code", "api_year", "student_unique_id").alias("k_student"),
        F.col("tenant_code"),
        F.col("school_year"),
        F.col("student_unique_id"),
        F.col("education_organization_id"),
        extract_descriptor(F.col("term_descriptor")).alias("term"),
        F.col("cumulative_gpa"),
        F.col("session_gpa"),
        F.col("cumulative_attempted_credits").cast("double").alias("cumulative_attempted_credits"),
        F.col("cumulative_earned_credits").cast("double").alias("cumulative_earned_credits"),
    )


@dlt.table(name="fct_student_gpa",
           comment="Cumulative-GPA fact derived from latest academic record. Replaces edu_wh.fct_student_gpa.",
           table_properties={"quality": "gold"})
def fct_student_gpa():
    ar = _gpa_columns(_silver("student_academic_records"))
    from pyspark.sql.window import Window
    latest = (ar.withColumn("_rn", F.row_number().over(
        Window.partitionBy("tenant_code", "api_year", "student_unique_id")
              .orderBy(
                  _term_order(extract_descriptor(F.col("term_descriptor"))).desc(),
                  F.col("pull_change_version").desc(),
              ))
    ).filter("_rn = 1").drop("_rn"))
    return latest.select(
        k_student("tenant_code", "api_year", "student_unique_id").alias("k_student"),
        F.col("tenant_code"),
        F.col("school_year"),
        F.col("student_unique_id"),
        F.col("cumulative_gpa"),
    )


@dlt.table(name="fct_course_transcripts",
           comment="Course transcript fact (one row per attempted course). Replaces edu_wh.fct_course_transcripts.",
           table_properties={"quality": "gold"})
def fct_course_transcripts():
    ct = _silver("course_transcripts")
    return ct.select(
        k_student("tenant_code", "api_year", "student_unique_id").alias("k_student"),
        k_course("tenant_code", "api_year", "course_code", "course_education_organization_id").alias("k_course"),
        F.col("tenant_code"),
        F.col("school_year"),
        F.col("student_unique_id"),
        extract_descriptor(F.col("term_descriptor")).alias("term"),
        F.col("course_code"),
        F.col("course_education_organization_id"),
        extract_descriptor(F.col("course_attempt_result_descriptor")).alias("course_attempt_result"),
        F.col("final_letter_grade_earned"),
        F.col("final_numeric_grade_earned").cast("double").alias("final_numeric_grade_earned"),
        F.col("attempted_credits").cast("double").alias("attempted_credits"),
        F.col("earned_credits").cast("double").alias("earned_credits"),
    )


@dlt.table(name="fct_student_section_attendance_event",
           comment="Section-level attendance fact. Replaces edu_wh.fct_student_section_attendance_event.",
           table_properties={"quality": "gold"})
def fct_student_section_attendance_event():
    ev = _silver("student_section_attendance_events")
    return ev.select(
        k_student("tenant_code", "api_year", "student_unique_id").alias("k_student"),
        k_section("tenant_code", "school_id", "school_year", "session_name",
                   "local_course_code", "section_identifier").alias("k_course_section"),
        F.col("tenant_code"),
        F.col("school_year"),
        F.col("student_unique_id"),
        F.col("school_id"),
        F.col("session_name"),
        F.col("local_course_code"),
        F.col("section_identifier"),
        F.col("attendance_event_date").cast("date").alias("attendance_event_date"),
        extract_descriptor(F.col("attendance_event_category_descriptor")).alias("attendance_event_category"),
        F.when(extract_descriptor(F.col("attendance_event_category_descriptor")).rlike("(?i)Absence"),
               F.lit(True)).otherwise(F.lit(False)).alias("is_absent"),
    )


@dlt.table(name="fct_student_daily_attendance",
           comment="Daily attendance roll-up (1 row per student/date). Replaces edu_wh.fct_student_daily_attendance.",
           table_properties={"quality": "gold"})
def fct_student_daily_attendance():
    ev = _silver("student_school_attendance_events")
    return (
        ev.groupBy("tenant_code", "api_year", "student_unique_id", "school_id",
                    F.col("attendance_event_date").cast("date").alias("attendance_event_date"))
          .agg(F.max(
                F.when(extract_descriptor(F.col("attendance_event_category_descriptor")).rlike("(?i)Absence"),
                       F.lit(True)).otherwise(F.lit(False))
              ).alias("is_absent"))
          .select(
            k_student("tenant_code", "api_year", "student_unique_id").alias("k_student"),
            k_school("tenant_code", "api_year", "school_id").alias("k_school"),
            F.col("tenant_code"), F.col("api_year").alias("school_year"),
            F.col("student_unique_id"),
            F.col("school_id"),
            F.col("attendance_event_date"),
            F.col("is_absent"),
          )
    )


@dlt.table(name="msr_student_cumulative_attendance",
           comment="Cumulative attendance measure per student-year. Replaces edu_wh.msr_student_cumulative_attendance.",
           table_properties={"quality": "gold"})
def msr_student_cumulative_attendance():
    daily = dlt.read("fct_student_daily_attendance")
    # Tunable chronic-absence rule (EDU/TN defaults: present < 90% over >= 20 days).
    threshold = float(_conf("edu.chronic_absence_threshold", "90"))
    min_days = int(_conf("edu.chronic_absence_min_days", "20"))
    return (
        daily.groupBy("tenant_code", "school_year", "k_student", "student_unique_id", "k_school", "school_id")
             .agg(
                F.count("*").alias("total_days"),
                F.sum(F.col("is_absent").cast("int")).alias("days_absent"),
                F.sum((~F.col("is_absent")).cast("int")).alias("days_present"),
             )
             .withColumn("attendance_rate",
                         F.when(F.col("total_days") > 0,
                                F.col("days_present") / F.col("total_days")
                         ).otherwise(F.lit(None)))
             .withColumn("is_chronically_absent",
                         chronic_absent_flag(F.col("attendance_rate"), F.col("total_days"), threshold, min_days))
    )


@dlt.table(name="fct_student_discipline_actions_summary",
           comment="Per-student count of discipline actions. Replaces edu_wh.fct_student_discipline_actions_summary.",
           table_properties={"quality": "gold"})
def fct_student_discipline_actions_summary():
    da = dlt.read("fct_student_discipline_actions")
    return (
        da.groupBy("tenant_code", "school_year", "k_student", "student_unique_id")
          .agg(F.count("*").alias("discipline_action_count"),
               F.min("discipline_date").alias("first_action_date"),
               F.max("discipline_date").alias("last_action_date"))
    )


@dlt.table(name="fct_student_discipline_incident_summary",
           comment="Per-student count of discipline incidents. Replaces edu_wh.fct_student_discipline_incident_summary.",
           table_properties={"quality": "gold"})
def fct_student_discipline_incident_summary():
    # Offender participation: the legacy association table (< DS 5.x) unioned
    # with behavior associations (DS 5.x replacement), de-duplicated per
    # student-incident.
    cols = ["tenant_code", "api_year", "student_unique_id", "incident_identifier", "school_id"]
    legacy = _silver("student_discipline_incident_associations").select(*cols)
    behavior = _silver("student_discipline_incident_behavior_associations").select(*cols)
    a = legacy.unionByName(behavior).distinct()
    return (
        a.groupBy("tenant_code", "api_year", "student_unique_id")
         .agg(F.count("*").alias("incident_count"))
         .select(
            k_student("tenant_code", "api_year", "student_unique_id").alias("k_student"),
            F.col("tenant_code"),
            F.col("api_year").alias("school_year"),
            F.col("student_unique_id"),
            F.col("incident_count"),
         )
    )


@dlt.table(name="fct_student_discipline_incident_behaviors",
           comment="Behavior detail per student incident. Replaces edu_wh.fct_student_discipline_incident_behaviors.",
           table_properties={"quality": "gold"})
def fct_student_discipline_incident_behaviors():
    b = _silver("student_discipline_incident_behavior_associations")
    return b.select(
        k_student("tenant_code", "api_year", "student_unique_id").alias("k_student"),
        F.col("tenant_code"),
        F.col("api_year").alias("school_year"),
        F.col("student_unique_id"),
        F.col("incident_identifier"),
        F.col("school_id"),
        extract_descriptor(F.col("behavior_descriptor")).alias("behavior"),
    )


@dlt.table(name="fct_student_discipline_incident_non_offenders",
           comment="Non-offender involvement per incident. Replaces edu_wh.fct_student_discipline_incident_non_offenders.",
           table_properties={"quality": "gold"})
def fct_student_discipline_incident_non_offenders():
    no = _silver("student_discipline_incident_non_offender_associations")
    return no.select(
        k_student("tenant_code", "api_year", "student_unique_id").alias("k_student"),
        F.col("tenant_code"),
        F.col("api_year").alias("school_year"),
        F.col("student_unique_id"),
        F.col("incident_identifier"),
        F.col("school_id"),
    )


@dlt.table(name="fct_staff_section_association",
           comment="Staff-section assignment fact. Replaces edu_wh.fct_staff_section_association.",
           table_properties={"quality": "gold"})
def fct_staff_section_association():
    ssa = _silver("staff_section_associations")
    return ssa.select(
        k_staff("tenant_code", "api_year", "staff_unique_id").alias("k_staff"),
        k_section("tenant_code", "school_id", "school_year", "session_name",
                   "local_course_code", "section_identifier").alias("k_course_section"),
        F.col("tenant_code"),
        F.col("api_year").alias("school_year"),
        F.col("staff_unique_id"),
        F.col("school_id"),
        F.col("session_name"),
        F.col("local_course_code"),
        F.col("section_identifier"),
        extract_descriptor(F.col("classroom_position_descriptor")).alias("classroom_position"),
    )


@dlt.table(name="fct_student_subgroup",
           comment="Student-subgroup flags derived from program participation. Replaces edu_wh.fct_student_subgroup.",
           table_properties={"quality": "gold"})
def fct_student_subgroup():
    # build a row per student with boolean program flags
    students = _silver("students").select("tenant_code", "api_year", "student_unique_id").distinct()
    def flag(table: str, col: str):
        # Active membership only (open as of academic year-end): exited students
        # are NOT counted, so subgroup counts aren't inflated by prior participation.
        try:
            df = _silver(table)
            begin = F.col("begin_date").cast("date")
            end = (F.col("end_date").cast("date") if "end_date" in df.columns else F.lit(None).cast("date"))
            _, is_active = program_window_flags(begin, end, F.col("api_year"))
            return (df.where(is_active)
                      .select("tenant_code", "api_year", "student_unique_id", F.lit(True).alias(col))
                      .distinct())
        except Exception:
            return students.withColumn(col, F.lit(False))

    sped = flag("student_special_education_program_associations", "is_special_education")
    ell  = flag("student_language_instruction_program_associations", "is_english_learner")
    home = flag("student_homeless_program_associations", "is_homeless")
    t1a  = flag("student_title_i_part_a_program_associations", "is_title_i")
    cte  = flag("student_cte_program_associations", "is_cte")
    meps = flag("student_migrant_education_program_associations", "is_migrant")
    fs   = flag("student_school_food_service_program_association", "is_food_service")
    return (
        students
        .join(sped, ["tenant_code","api_year","student_unique_id"], "left")
        .join(ell,  ["tenant_code","api_year","student_unique_id"], "left")
        .join(home, ["tenant_code","api_year","student_unique_id"], "left")
        .join(t1a,  ["tenant_code","api_year","student_unique_id"], "left")
        .join(cte,  ["tenant_code","api_year","student_unique_id"], "left")
        .join(meps, ["tenant_code","api_year","student_unique_id"], "left")
        .join(fs,   ["tenant_code","api_year","student_unique_id"], "left")
        .select(
            k_student("tenant_code", "api_year", "student_unique_id").alias("k_student"),
            F.col("tenant_code"), F.col("api_year").alias("school_year"),
            F.col("student_unique_id"),
            F.coalesce("is_special_education", F.lit(False)).alias("is_special_education"),
            F.coalesce("is_english_learner", F.lit(False)).alias("is_english_learner"),
            F.coalesce("is_homeless", F.lit(False)).alias("is_homeless"),
            F.coalesce("is_title_i", F.lit(False)).alias("is_title_i"),
            F.coalesce("is_cte", F.lit(False)).alias("is_cte"),
            F.coalesce("is_migrant", F.lit(False)).alias("is_migrant"),
            F.coalesce("is_food_service", F.lit(False)).alias("is_food_service"),
        )
    )


@dlt.table(name="brg_course_section_program",
           comment="Course-section to program bridge. Replaces edu_wh.brg_course_section_program.",
           table_properties={"quality": "gold"})
def brg_course_section_program():
    # Real bridge: explode sections.programs[] (programReference) so each row
    # links a section to a program — EDU's brg_course_section_program.
    from pyspark.sql.types import ArrayType, StructType, StructField, StringType
    prog_schema = ArrayType(StructType([
        StructField("programReference", StructType([
            StructField("educationOrganizationId", StringType()),
            StructField("programName", StringType()),
            StructField("programTypeDescriptor", StringType()),
        ])),
    ]))
    secs = _silver("sections")
    return (
        secs.withColumn("_progs", F.from_json(F.get_json_object(F.col("payload"), "$.programs"), prog_schema))
            .where(F.col("_progs").isNotNull() & (F.size("_progs") > 0))
            .withColumn("_p", F.explode("_progs"))
            .select(
                k_section("tenant_code", "school_id", "school_year", "session_name",
                           "local_course_code", "section_identifier").alias("k_course_section"),
                k_program(
                    "tenant_code", "api_year",
                    F.col("_p.programReference.programName"),
                    extract_descriptor(F.col("_p.programReference.programTypeDescriptor")),
                    F.col("_p.programReference.educationOrganizationId"),
                ).alias("k_program"),
                F.col("tenant_code"), F.col("school_id"), F.col("school_year"),
                F.col("session_name"), F.col("local_course_code"), F.col("section_identifier"),
                F.col("_p.programReference.programName").alias("program_name"),
                extract_descriptor(F.col("_p.programReference.programTypeDescriptor")).alias("program_type"),
            )
    )


# ============================================================
# REMAINING EDU MARTS — closes the gap to 100% core_warehouse parity
# ============================================================

@dlt.table(name="fct_student_diploma",
           comment="Diploma awards. Replaces edu_wh.fct_student_diploma. Derives from student_academic_records where a diploma was awarded.",
           table_properties={"quality": "gold"})
def fct_student_diploma():
    ar = _gpa_columns(_silver("student_academic_records"))
    # Extract the first diploma from the payload's diplomas[] array (typical Ed-Fi shape).
    return (
        ar.select(
            k_student("tenant_code", "api_year", "student_unique_id").alias("k_student"),
            F.col("tenant_code"),
            F.col("school_year"),
            F.col("student_unique_id"),
            F.col("education_organization_id"),
            extract_descriptor(F.col("term_descriptor")).alias("term"),
            F.get_json_object(F.col("payload"), "$.diplomas[0].diplomaAwardDate").cast("date").alias("diploma_award_date"),
            extract_descriptor(F.get_json_object(F.col("payload"), "$.diplomas[0].diplomaTypeDescriptor")).alias("diploma_type"),
            extract_descriptor(F.get_json_object(F.col("payload"), "$.diplomas[0].diplomaLevelDescriptor")).alias("diploma_level"),
            F.col("cumulative_gpa"),
            F.col("cumulative_earned_credits").cast("double").alias("cumulative_earned_credits"),
        )
        .where(F.col("diploma_award_date").isNotNull())
    )


@dlt.table(name="fct_student_learning_standard_grades",
           comment="Per-learning-standard grade results. Replaces edu_wh.fct_student_learning_standard_grades. Explodes the gradeLearningStandards array from each grade record.",
           table_properties={"quality": "gold"})
def fct_student_learning_standard_grades():
    # Real DS 5.x shape: grades.learningStandardGrades[] with a
    # learningStandardReference (validated against the 7.3 OpenAPI spec).
    from pyspark.sql.types import ArrayType, StructType, StructField, StringType
    arr_schema = ArrayType(StructType([
        StructField("learningStandardReference", StructType([
            StructField("learningStandardId", StringType()),
        ])),
        StructField("diagnosticStatement", StringType()),
        StructField("performanceBaseConversionDescriptor", StringType()),
        StructField("letterGradeEarned", StringType()),
        StructField("numericGradeEarned", StringType()),
    ]))

    g = _silver("grades")
    return (
        g.withColumn("_lsg", F.from_json(F.get_json_object(F.col("payload"), "$.learningStandardGrades"), arr_schema))
         .where(F.col("_lsg").isNotNull() & (F.size("_lsg") > 0))
         .withColumn("_lsg1", F.explode("_lsg"))
         .select(
            k_student("tenant_code", "api_year", "student_unique_id").alias("k_student"),
            k_section("tenant_code", "school_id", "school_year", "session_name",
                       "local_course_code", "section_identifier").alias("k_course_section"),
            k_learning_standard("tenant_code", F.col("_lsg1.learningStandardReference.learningStandardId")).alias("k_learning_standard"),
            F.col("tenant_code"),
            F.col("school_year"),
            F.col("student_unique_id"),
            F.col("school_id"),
            F.col("local_course_code"),
            F.col("section_identifier"),
            F.col("_lsg1.learningStandardReference.learningStandardId").alias("learning_standard_id"),
            F.col("_lsg1.diagnosticStatement").alias("diagnostic_statement"),
            extract_descriptor(F.col("_lsg1.performanceBaseConversionDescriptor")).alias("performance_level"),
            F.col("numeric_grade_earned").cast("double").alias("numeric_grade_earned"),
        )
    )


@dlt.table(name="fct_student_objective_assessment",
           comment="Per-objective-assessment student score. Replaces edu_wh.fct_student_objective_assessment. Explodes studentObjectiveAssessments[] from each student_assessment.",
           table_properties={"quality": "gold"})
def fct_student_objective_assessment():
    from pyspark.sql.types import ArrayType, StructType, StructField, StringType
    arr_schema = ArrayType(StructType([
        StructField("objectiveAssessmentReference", StructType([
            StructField("assessmentIdentifier", StringType()),
            StructField("namespace", StringType()),
            StructField("identificationCode", StringType()),
        ])),
        StructField("scoreResults", ArrayType(StructType([
            StructField("result", StringType()),
            StructField("assessmentReportingMethodDescriptor", StringType()),
        ]))),
    ]))

    sa = _silver("student_assessments")
    return (
        sa.withColumn("_objs", F.from_json(F.get_json_object(F.col("payload"), "$.studentObjectiveAssessments"), arr_schema))
          .where(F.col("_objs").isNotNull() & (F.size("_objs") > 0))
          .withColumn("_o", F.explode("_objs"))
          .select(
            k_student("tenant_code", "api_year", "student_unique_id").alias("k_student"),
            k_objective_assessment(
                "tenant_code",
                F.col("_o.objectiveAssessmentReference.assessmentIdentifier"),
                F.col("_o.objectiveAssessmentReference.identificationCode"),
                F.col("_o.objectiveAssessmentReference.namespace"),
            ).alias("k_objective_assessment"),
            k_assessment(
                "tenant_code",
                F.col("_o.objectiveAssessmentReference.assessmentIdentifier"),
                F.col("_o.objectiveAssessmentReference.namespace"),
            ).alias("k_assessment"),
            F.col("tenant_code"),
            F.col("api_year").alias("school_year"),
            F.col("student_unique_id"),
            F.col("_o.objectiveAssessmentReference.assessmentIdentifier").alias("assessment_identifier"),
            F.col("_o.objectiveAssessmentReference.identificationCode").alias("identification_code"),
            F.col("_o.objectiveAssessmentReference.namespace").alias("namespace"),
            F.element_at(F.col("_o.scoreResults"), 1).getField("result").alias("score_result"),
            extract_descriptor(F.element_at(F.col("_o.scoreResults"), 1).getField("assessmentReportingMethodDescriptor")).alias("score_reporting_method"),
        )
    )


@dlt.table(name="fct_student_program_service",
           comment="Specific services delivered under a student-program association. Replaces edu_wh.fct_student_program_service. Explodes services[] from each student_program_association.",
           table_properties={"quality": "gold"})
def fct_student_program_service():
    from pyspark.sql.types import ArrayType, StructType, StructField, StringType, BooleanType
    svc_schema = ArrayType(StructType([
        StructField("serviceDescriptor", StringType()),
        StructField("primaryIndicator", BooleanType()),
        StructField("serviceBeginDate", StringType()),
        StructField("serviceEndDate", StringType()),
    ]))

    p = _silver("student_program_associations")
    return (
        p.withColumn("_svcs", F.from_json(F.get_json_object(F.col("payload"), "$.services"), svc_schema))
         .where(F.col("_svcs").isNotNull() & (F.size("_svcs") > 0))
         .withColumn("_s", F.explode("_svcs"))
         .select(
            k_student("tenant_code", "api_year", "student_unique_id").alias("k_student"),
            k_program(
                "tenant_code", "api_year", "program_name",
                extract_descriptor(F.col("program_type_descriptor")),
                "education_organization_id",
            ).alias("k_program"),
            F.col("tenant_code"),
            F.col("api_year").alias("school_year"),
            F.col("student_unique_id"),
            F.col("education_organization_id"),
            F.col("program_name"),
            extract_descriptor(F.col("program_type_descriptor")).alias("program_type"),
            extract_descriptor(F.col("_s.serviceDescriptor")).alias("service"),
            F.col("_s.primaryIndicator").cast("boolean").alias("is_primary"),
            F.col("_s.serviceBeginDate").cast("date").alias("service_begin_date"),
            F.col("_s.serviceEndDate").cast("date").alias("service_end_date"),
        )
    )


# ============================================================
# TPDM (Teacher Preparation Data Model) — replaces edu_wh.tpdm_warehouse.*
# ============================================================

@dlt.table(name="dim_candidate",
           comment="Educator candidate dim. Replaces edu_wh.tpdm_warehouse.dim_candidate.",
           table_properties={"quality": "gold"})
def dim_candidate():
    c = _silver("candidates")
    return c.select(
        k_candidate("tenant_code", "api_year", "candidate_identifier").alias("k_candidate"),
        F.col("tenant_code"),
        F.col("api_year").alias("school_year"),
        F.col("candidate_identifier"),
        F.col("first_name"),
        F.col("last_name"),
        F.concat_ws(" ", "first_name", "last_name").alias("display_name"),
        F.col("birth_date").cast("date").alias("birth_date"),
        extract_descriptor(F.col("sex_descriptor")).alias("gender"),
    )


@dlt.table(name="dim_educator_preparation_program",
           comment="Educator-prep program dim. Replaces edu_wh.tpdm_warehouse.dim_educator_preparation_program.",
           table_properties={"quality": "gold"})
def dim_educator_preparation_program():
    ep = _silver("educator_preparation_programs")
    return ep.select(
        k_educator_prep_program(
            "tenant_code", "api_year", "program_name",
            extract_descriptor(F.col("program_type_descriptor")),
            "education_organization_id",
        ).alias("k_educator_prep_program"),
        F.col("tenant_code"),
        F.col("api_year").alias("school_year"),
        F.col("program_name"),
        extract_descriptor(F.col("program_type_descriptor")).alias("program_type"),
        F.col("education_organization_id"),
    )


@dlt.table(name="fct_candidate_educator_preparation_program_association",
           comment="Candidate↔prep-program association fact. Replaces edu_wh.tpdm_warehouse.fct_candidate_educator_preparation_program_association.",
           table_properties={"quality": "gold"})
def fct_candidate_educator_preparation_program_association():
    a = _silver("candidate_educator_preparation_program_associations")
    return a.select(
        k_candidate("tenant_code", "api_year", "candidate_identifier").alias("k_candidate"),
        k_educator_prep_program(
            "tenant_code", "api_year", "program_name",
            extract_descriptor(F.col("program_type_descriptor")),
            "education_organization_id",
        ).alias("k_educator_prep_program"),
        F.col("tenant_code"),
        F.col("api_year").alias("school_year"),
        F.col("candidate_identifier"),
        F.col("program_name"),
        extract_descriptor(F.col("program_type_descriptor")).alias("program_type"),
        F.col("education_organization_id"),
        F.col("begin_date").cast("date").alias("begin_date"),
        F.col("end_date").cast("date").alias("end_date"),
        extract_descriptor(F.col("ep_program_pathway_descriptor")).alias("ep_program_pathway"),
    )
