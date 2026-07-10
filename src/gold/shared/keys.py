"""
Surrogate-key conventions, shared across all gold marts.

These mirror the `dbt_utils.generate_surrogate_key()` inputs used in
edu_wh/macros/keys.sql so the warehouse joins line up 1:1 with EDU.
"""
from __future__ import annotations

from pyspark.sql import Column, functions as F

NULL_MARKER = "_dbt_utils_surrogate_key_null_"


def _sk(*cols) -> Column:
    parts = []
    for c in cols:
        c = F.col(c) if isinstance(c, str) else c
        parts.append(F.lower(F.coalesce(c.cast("string"), F.lit(NULL_MARKER))))
    return F.md5(F.concat_ws("-", *parts))


def k_student(tenant_code, api_year, student_unique_id) -> Column:
    return _sk(tenant_code, api_year, F.lower(F.col(student_unique_id) if isinstance(student_unique_id, str) else student_unique_id))


def k_student_xyear(tenant_code, student_unique_id) -> Column:
    return _sk(tenant_code, F.lower(F.col(student_unique_id) if isinstance(student_unique_id, str) else student_unique_id))


def k_school(tenant_code, api_year, school_id) -> Column:
    return _sk(tenant_code, api_year, school_id)


def k_lea(tenant_code, api_year, lea_id) -> Column:
    return _sk(tenant_code, api_year, lea_id)


def k_session(tenant_code, school_id, school_year, session_name) -> Column:
    return _sk(tenant_code, school_id, school_year, session_name)


def k_course(tenant_code, api_year, course_code, education_organization_id) -> Column:
    return _sk(tenant_code, api_year, course_code, education_organization_id)


def k_section(tenant_code, school_id, school_year, session_name, local_course_code, section_identifier) -> Column:
    return _sk(tenant_code, school_id, school_year, session_name, local_course_code, section_identifier)


def k_staff(tenant_code, api_year, staff_unique_id) -> Column:
    return _sk(tenant_code, api_year, F.lower(F.col(staff_unique_id) if isinstance(staff_unique_id, str) else staff_unique_id))


def k_grading_period(tenant_code, school_id, school_year, grading_period_name) -> Column:
    # DS 5.x keys grading periods by gradingPeriodName (periodSequence is no
    # longer carried in gradingPeriodReference). Callers pass
    # coalesce(grading_period_name, extracted descriptor) so the same key is
    # computed on both the dim and fact sides regardless of DS version —
    # mirrors EDU's data-standard-versioned k_grading_period.
    return _sk(tenant_code, school_id, school_year, grading_period_name)


def k_calendar_date(tenant_code, school_id, school_year, calendar_code, calendar_date) -> Column:
    return _sk(tenant_code, school_id, school_year, calendar_code, calendar_date)


def k_assessment(tenant_code, assessment_identifier, namespace) -> Column:
    return _sk(tenant_code, assessment_identifier, namespace)


def k_program(tenant_code, api_year, program_name, program_type, education_organization_id) -> Column:
    return _sk(tenant_code, api_year, program_name, program_type, education_organization_id)


def k_cohort(tenant_code, api_year, cohort_identifier, education_organization_id) -> Column:
    return _sk(tenant_code, api_year, cohort_identifier, education_organization_id)


def k_parent(tenant_code, api_year, parent_unique_id) -> Column:
    return _sk(tenant_code, api_year, F.lower(F.col(parent_unique_id) if isinstance(parent_unique_id, str) else parent_unique_id))


def k_class_period(tenant_code, api_year, class_period_name, school_id) -> Column:
    return _sk(tenant_code, api_year, class_period_name, school_id)


def k_classroom(tenant_code, api_year, classroom_id_code, school_id) -> Column:
    return _sk(tenant_code, api_year, classroom_id_code, school_id)


def k_esc(tenant_code, api_year, education_service_center_id) -> Column:
    return _sk(tenant_code, api_year, education_service_center_id)


def k_network(tenant_code, api_year, network_id) -> Column:
    return _sk(tenant_code, api_year, network_id)


def k_learning_standard(tenant_code, learning_standard_id) -> Column:
    return _sk(tenant_code, learning_standard_id)


def k_objective_assessment(tenant_code, assessment_identifier, identification_code, namespace) -> Column:
    return _sk(tenant_code, assessment_identifier, identification_code, namespace)


def k_graduation_plan(tenant_code, education_organization_id, graduation_plan_type, graduation_school_year) -> Column:
    return _sk(tenant_code, education_organization_id, graduation_plan_type, graduation_school_year)


def k_school_calendar(tenant_code, school_id, school_year, calendar_code) -> Column:
    return _sk(tenant_code, school_id, school_year, calendar_code)


# ----- TPDM keys -----
def k_candidate(tenant_code, api_year, candidate_identifier) -> Column:
    return _sk(tenant_code, api_year, F.lower(F.col(candidate_identifier) if isinstance(candidate_identifier, str) else candidate_identifier))


def k_educator_prep_program(tenant_code, api_year, program_name, program_type, education_organization_id) -> Column:
    return _sk(tenant_code, api_year, program_name, program_type, education_organization_id)
