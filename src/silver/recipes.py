"""
Silver extraction recipes: Silver column -> JSON path into bronze.payload.

Kept separate from pipeline.py so unit tests can validate every path against
the synthetic generator WITHOUT importing the dlt runtime. All paths are
validated against the Ed-Fi 7.3 OpenAPI spec; a recipe column that doesn't
resolve on real data is a bug, not a tolerated fallback.
"""
from __future__ import annotations

from typing import Dict

# Schema/extraction recipes for resources gold actually uses. Each entry maps
# a Silver column -> JSON path into bronze.payload. All paths are validated
# against the Ed-Fi 7.3 spec (see docs/MART_PARITY.md for DS-version notes).
# Resources not listed here still get a *_base / stage pair with metadata +
# payload columns, and their dedupe keys come from manifest key_paths.
RECIPES: Dict[str, Dict[str, str]] = {
    "students": {
        "student_unique_id":   "$.studentUniqueId",
        "first_name":          "$.firstName",
        "middle_name":         "$.middleName",
        "last_name":           "$.lastSurname",
        "birth_date":          "$.birthDate",
        "birth_city":          "$.birthCity",
        "birth_sex_descriptor": "$.birthSexDescriptor",
    },
    "schools": {
        "school_id":           "$.schoolId",
        "name_of_institution": "$.nameOfInstitution",
        "lea_id":              "$.localEducationAgencyReference.localEducationAgencyId",
    },
    "local_education_agencies": {
        "lea_id":              "$.localEducationAgencyId",
        "name_of_institution": "$.nameOfInstitution",
        "lea_category_descriptor": "$.localEducationAgencyCategoryDescriptor",
    },
    "staffs": {
        "staff_unique_id":     "$.staffUniqueId",
        "first_name":          "$.firstName",
        "last_name":           "$.lastSurname",
        "birth_date":          "$.birthDate",
        "highest_completed_level_of_education_descriptor": "$.highestCompletedLevelOfEducationDescriptor",
    },
    "sessions": {
        "school_id":           "$.schoolReference.schoolId",
        "school_year":         "$.schoolYearTypeReference.schoolYear",
        "session_name":        "$.sessionName",
        "begin_date":          "$.beginDate",
        "end_date":            "$.endDate",
        "total_instructional_days": "$.totalInstructionalDays",
        "term_descriptor":     "$.termDescriptor",
    },
    "grading_periods": {
        "school_id":           "$.schoolReference.schoolId",
        "school_year":         "$.schoolYearTypeReference.schoolYear",
        "grading_period_descriptor": "$.gradingPeriodDescriptor",
        "grading_period_name": "$.gradingPeriodName",
        "period_sequence":     "$.periodSequence",
        "begin_date":          "$.beginDate",
        "end_date":            "$.endDate",
    },
    "calendars": {
        "calendar_code":       "$.calendarCode",
        "school_id":           "$.schoolReference.schoolId",
        "school_year":         "$.schoolYearTypeReference.schoolYear",
        "calendar_type_descriptor": "$.calendarTypeDescriptor",
    },
    # DS 5.x: calendarDates carry a calendarReference, not school/year refs.
    "calendar_dates": {
        "calendar_code":       "$.calendarReference.calendarCode",
        "school_id":           "$.calendarReference.schoolId",
        "school_year":         "$.calendarReference.schoolYear",
        "calendar_date":       "$.date",
    },
    # DS 5.x: course subject moved into the academicSubjects[] array.
    "courses": {
        "course_code":         "$.courseCode",
        "education_organization_id": "$.educationOrganizationReference.educationOrganizationId",
        "course_title":        "$.courseTitle",
        "academic_subject_descriptor": "$.academicSubjects[0].academicSubjectDescriptor",
        "number_of_parts":     "$.numberOfParts",
    },
    "course_offerings": {
        "local_course_code":   "$.localCourseCode",
        "school_id":           "$.schoolReference.schoolId",
        "school_year":         "$.sessionReference.schoolYear",
        "session_name":        "$.sessionReference.sessionName",
        "course_code":         "$.courseReference.courseCode",
        "course_education_organization_id": "$.courseReference.educationOrganizationId",
        "local_course_title":  "$.localCourseTitle",
    },
    # DS 5.x: sections hang off a courseOfferingReference.
    "sections": {
        "local_course_code":   "$.courseOfferingReference.localCourseCode",
        "school_id":           "$.courseOfferingReference.schoolId",
        "school_year":         "$.courseOfferingReference.schoolYear",
        "session_name":        "$.courseOfferingReference.sessionName",
        "section_identifier":  "$.sectionIdentifier",
        "available_credits":   "$.availableCredits",
        "educational_environment_descriptor": "$.educationalEnvironmentDescriptor",
    },
    "student_school_associations": {
        "student_unique_id":   "$.studentReference.studentUniqueId",
        "school_id":           "$.schoolReference.schoolId",
        "school_year":         "$.schoolYearTypeReference.schoolYear",
        "entry_date":          "$.entryDate",
        "exit_withdraw_date":  "$.exitWithdrawDate",
        "entry_grade_level_descriptor": "$.entryGradeLevelDescriptor",
    },
    "student_section_associations": {
        "student_unique_id":   "$.studentReference.studentUniqueId",
        "local_course_code":   "$.sectionReference.localCourseCode",
        "school_id":           "$.sectionReference.schoolId",
        "school_year":         "$.sectionReference.schoolYear",
        "session_name":        "$.sectionReference.sessionName",
        "section_identifier":  "$.sectionReference.sectionIdentifier",
        "begin_date":          "$.beginDate",
        "end_date":            "$.endDate",
    },
    "student_education_organization_associations": {
        "student_unique_id":   "$.studentReference.studentUniqueId",
        "education_organization_id": "$.educationOrganizationReference.educationOrganizationId",
        "sex_descriptor":      "$.sexDescriptor",
        "limited_english_proficiency_descriptor": "$.limitedEnglishProficiencyDescriptor",
        "hispanic_latino_ethnicity": "$.hispanicLatinoEthnicity",
        "races_json":          "$.races",
    },
    "student_education_organization_responsibility_associations": {
        "student_unique_id":   "$.studentReference.studentUniqueId",
        "education_organization_id": "$.educationOrganizationReference.educationOrganizationId",
        "responsibility_descriptor": "$.responsibilityDescriptor",
        "begin_date":          "$.beginDate",
    },
    # DS 5.x: the attendance date field is `eventDate`.
    "student_school_attendance_events": {
        "student_unique_id":   "$.studentReference.studentUniqueId",
        "school_id":           "$.schoolReference.schoolId",
        "school_year":         "$.sessionReference.schoolYear",
        "session_name":        "$.sessionReference.sessionName",
        "attendance_event_date": "$.eventDate",
        "attendance_event_category_descriptor": "$.attendanceEventCategoryDescriptor",
        "attendance_event_reason": "$.attendanceEventReason",
        "event_duration":      "$.eventDuration",
    },
    # DS 5.x: grading periods are keyed by gradingPeriodName in references.
    "grades": {
        "student_unique_id":   "$.studentSectionAssociationReference.studentUniqueId",
        "local_course_code":   "$.studentSectionAssociationReference.localCourseCode",
        "school_id":           "$.studentSectionAssociationReference.schoolId",
        "school_year":         "$.studentSectionAssociationReference.schoolYear",
        "session_name":        "$.studentSectionAssociationReference.sessionName",
        "section_identifier":  "$.studentSectionAssociationReference.sectionIdentifier",
        "grading_period_descriptor": "$.gradingPeriodReference.gradingPeriodDescriptor",
        "grading_period_name": "$.gradingPeriodReference.gradingPeriodName",
        "grade_type_descriptor": "$.gradeTypeDescriptor",
        "numeric_grade_earned": "$.numericGradeEarned",
        "letter_grade_earned": "$.letterGradeEarned",
    },
    "assessments": {
        "assessment_identifier": "$.assessmentIdentifier",
        "namespace":             "$.namespace",
        "assessment_title":      "$.assessmentTitle",
        "assessment_category_descriptor": "$.assessmentCategoryDescriptor",
    },
    "student_assessments": {
        "student_unique_id":   "$.studentReference.studentUniqueId",
        "assessment_identifier": "$.assessmentReference.assessmentIdentifier",
        "namespace":             "$.assessmentReference.namespace",
        "student_assessment_identifier": "$.studentAssessmentIdentifier",
        "administration_date":   "$.administrationDate",
    },
    "discipline_incidents": {
        "incident_identifier": "$.incidentIdentifier",
        "school_id":           "$.schoolReference.schoolId",
        "incident_date":       "$.incidentDate",
        "incident_description": "$.incidentDescription",
    },
    # DS 5.x: discipline actions have assignment/responsibility school refs,
    # not a plain schoolReference. We expose both; gold uses responsibility.
    "discipline_actions": {
        "discipline_action_identifier": "$.disciplineActionIdentifier",
        "discipline_date":     "$.disciplineDate",
        "student_unique_id":   "$.studentReference.studentUniqueId",
        "assignment_school_id": "$.assignmentSchoolReference.schoolId",
        "responsibility_school_id": "$.responsibilitySchoolReference.schoolId",
    },
    "student_discipline_incident_associations": {  # legacy (< DS 5.x) only
        "student_unique_id":   "$.studentReference.studentUniqueId",
        "incident_identifier": "$.disciplineIncidentReference.incidentIdentifier",
        "school_id":           "$.disciplineIncidentReference.schoolId",
    },
    "staff_school_associations": {
        "staff_unique_id":     "$.staffReference.staffUniqueId",
        "school_id":           "$.schoolReference.schoolId",
        "program_assignment_descriptor": "$.programAssignmentDescriptor",
    },
    "staff_section_associations": {
        "staff_unique_id":     "$.staffReference.staffUniqueId",
        "local_course_code":   "$.sectionReference.localCourseCode",
        "school_id":           "$.sectionReference.schoolId",
        "school_year":         "$.sectionReference.schoolYear",
        "session_name":        "$.sectionReference.sessionName",
        "section_identifier":  "$.sectionReference.sectionIdentifier",
        "classroom_position_descriptor": "$.classroomPositionDescriptor",
    },
    "staff_education_organization_assignment_associations": {
        "staff_unique_id":     "$.staffReference.staffUniqueId",
        "education_organization_id": "$.educationOrganizationReference.educationOrganizationId",
        "staff_classification_descriptor": "$.staffClassificationDescriptor",
        "begin_date":          "$.beginDate",
        "position_title":      "$.positionTitle",
    },
    "staff_education_organization_employment_associations": {
        "staff_unique_id":     "$.staffReference.staffUniqueId",
        "education_organization_id": "$.educationOrganizationReference.educationOrganizationId",
        "employment_status_descriptor": "$.employmentStatusDescriptor",
        "hire_date":           "$.hireDate",
    },
    "programs": {
        "program_name":         "$.programName",
        "program_type_descriptor": "$.programTypeDescriptor",
        "education_organization_id": "$.educationOrganizationReference.educationOrganizationId",
    },
    "cohorts": {
        "cohort_identifier":    "$.cohortIdentifier",
        "education_organization_id": "$.educationOrganizationReference.educationOrganizationId",
        "cohort_description":   "$.cohortDescription",
        "cohort_type_descriptor": "$.cohortTypeDescriptor",
    },
    "parents": {  # legacy (< DS 5.x) only; bridged with contacts in gold
        "parent_unique_id":     "$.parentUniqueId",
        "first_name":           "$.firstName",
        "last_name":            "$.lastSurname",
        "sex_descriptor":       "$.sexDescriptor",
    },
    "contacts": {
        "contact_unique_id":    "$.contactUniqueId",
        "first_name":           "$.firstName",
        "last_name":            "$.lastSurname",
        "sex_descriptor":       "$.sexDescriptor",
    },
    "class_periods": {
        "class_period_name":    "$.classPeriodName",
        "school_id":            "$.schoolReference.schoolId",
    },
    "locations": {
        "classroom_identification_code": "$.classroomIdentificationCode",
        "school_id":            "$.schoolReference.schoolId",
        "maximum_number_of_seats": "$.maximumNumberOfSeats",
    },
    "education_service_centers": {
        "education_service_center_id": "$.educationServiceCenterId",
        "name_of_institution":  "$.nameOfInstitution",
    },
    "education_organization_networks": {
        "network_id":           "$.educationOrganizationNetworkId",
        "network_name":         "$.nameOfInstitution",
        "network_purpose_descriptor": "$.networkPurposeDescriptor",
    },
    # DS 5.x: learning-standard subject moved into academicSubjects[].
    "learning_standards": {
        "learning_standard_id": "$.learningStandardId",
        "description":          "$.description",
        "academic_subject_descriptor": "$.academicSubjects[0].academicSubjectDescriptor",
    },
    # DS 5.x: assessment id/namespace live in assessmentReference.
    "objective_assessments": {
        "assessment_identifier": "$.assessmentReference.assessmentIdentifier",
        "identification_code":  "$.identificationCode",
        "namespace":            "$.assessmentReference.namespace",
        "description":          "$.description",
    },
    "graduation_plans": {
        "education_organization_id": "$.educationOrganizationReference.educationOrganizationId",
        "graduation_plan_type_descriptor": "$.graduationPlanTypeDescriptor",
        "graduation_school_year": "$.graduationSchoolYearTypeReference.schoolYear",
        "total_required_credits": "$.totalRequiredCredits",
    },
    "student_program_associations": {
        "student_unique_id":    "$.studentReference.studentUniqueId",
        "education_organization_id": "$.educationOrganizationReference.educationOrganizationId",
        "program_name":         "$.programReference.programName",
        "program_type_descriptor": "$.programReference.programTypeDescriptor",
        "begin_date":           "$.beginDate",
        "end_date":             "$.endDate",
    },
    # DS 5.x: the field is `ideaEligibility` (camelCase).
    "student_special_education_program_associations": {
        "student_unique_id":    "$.studentReference.studentUniqueId",
        "education_organization_id": "$.educationOrganizationReference.educationOrganizationId",
        "program_name":         "$.programReference.programName",
        "begin_date":           "$.beginDate",
        "end_date":             "$.endDate",
        "idea_eligibility":     "$.ideaEligibility",
    },
    "student_homeless_program_associations": {
        "student_unique_id":    "$.studentReference.studentUniqueId",
        "education_organization_id": "$.educationOrganizationReference.educationOrganizationId",
        "program_name":         "$.programReference.programName",
        "begin_date":           "$.beginDate",
        "end_date":             "$.endDate",
    },
    "student_language_instruction_program_associations": {
        "student_unique_id":    "$.studentReference.studentUniqueId",
        "education_organization_id": "$.educationOrganizationReference.educationOrganizationId",
        "program_name":         "$.programReference.programName",
        "begin_date":           "$.beginDate",
        "end_date":             "$.endDate",
    },
    "student_title_i_part_a_program_associations": {
        "student_unique_id":    "$.studentReference.studentUniqueId",
        "education_organization_id": "$.educationOrganizationReference.educationOrganizationId",
        "program_name":         "$.programReference.programName",
        "begin_date":           "$.beginDate",
        "end_date":             "$.endDate",
    },
    "student_cte_program_associations": {
        "student_unique_id":    "$.studentReference.studentUniqueId",
        "education_organization_id": "$.educationOrganizationReference.educationOrganizationId",
        "program_name":         "$.programReference.programName",
        "begin_date":           "$.beginDate",
        "end_date":             "$.endDate",
    },
    "student_migrant_education_program_associations": {
        "student_unique_id":    "$.studentReference.studentUniqueId",
        "education_organization_id": "$.educationOrganizationReference.educationOrganizationId",
        "program_name":         "$.programReference.programName",
        "begin_date":           "$.beginDate",
        "end_date":             "$.endDate",
    },
    "student_school_food_service_program_association": {
        "student_unique_id":    "$.studentReference.studentUniqueId",
        "education_organization_id": "$.educationOrganizationReference.educationOrganizationId",
        "program_name":         "$.programReference.programName",
        "begin_date":           "$.beginDate",
        "end_date":             "$.endDate",
    },
    "student_parent_associations": {  # legacy (< DS 5.x) only
        "student_unique_id":    "$.studentReference.studentUniqueId",
        "parent_unique_id":     "$.parentReference.parentUniqueId",
        "relation_descriptor":  "$.relationDescriptor",
        "primary_contact_status": "$.primaryContactStatus",
    },
    "student_contact_associations": {
        "student_unique_id":    "$.studentReference.studentUniqueId",
        "contact_unique_id":    "$.contactReference.contactUniqueId",
        "relation_descriptor":  "$.relationDescriptor",
        "primary_contact_status": "$.primaryContactStatus",
    },
    "student_cohort_associations": {
        "student_unique_id":    "$.studentReference.studentUniqueId",
        "cohort_identifier":    "$.cohortReference.cohortIdentifier",
        "education_organization_id": "$.cohortReference.educationOrganizationId",
        "begin_date":           "$.beginDate",
        "end_date":             "$.endDate",
    },
    # DS 5.x: GPA moved into gradePointAverages[]; extracted in gold.
    "student_academic_records": {
        "student_unique_id":    "$.studentReference.studentUniqueId",
        "education_organization_id": "$.educationOrganizationReference.educationOrganizationId",
        "school_year":          "$.schoolYearTypeReference.schoolYear",
        "term_descriptor":      "$.termDescriptor",
        "cumulative_attempted_credits": "$.cumulativeAttemptedCredits",
        "cumulative_earned_credits": "$.cumulativeEarnedCredits",
    },
    "course_transcripts": {
        "student_unique_id":    "$.studentAcademicRecordReference.studentUniqueId",
        "school_year":          "$.studentAcademicRecordReference.schoolYear",
        "term_descriptor":      "$.studentAcademicRecordReference.termDescriptor",
        "course_code":          "$.courseReference.courseCode",
        "course_education_organization_id": "$.courseReference.educationOrganizationId",
        "course_attempt_result_descriptor": "$.courseAttemptResultDescriptor",
        "final_letter_grade_earned": "$.finalLetterGradeEarned",
        "final_numeric_grade_earned": "$.finalNumericGradeEarned",
        "attempted_credits":    "$.attemptedCredits",
        "earned_credits":       "$.earnedCredits",
    },
    "student_section_attendance_events": {
        "student_unique_id":    "$.studentReference.studentUniqueId",
        "local_course_code":    "$.sectionReference.localCourseCode",
        "school_id":            "$.sectionReference.schoolId",
        "school_year":          "$.sectionReference.schoolYear",
        "session_name":         "$.sectionReference.sessionName",
        "section_identifier":   "$.sectionReference.sectionIdentifier",
        "attendance_event_date": "$.eventDate",
        "attendance_event_category_descriptor": "$.attendanceEventCategoryDescriptor",
    },
    "student_discipline_incident_behavior_associations": {
        "student_unique_id":    "$.studentReference.studentUniqueId",
        "incident_identifier":  "$.disciplineIncidentReference.incidentIdentifier",
        "school_id":            "$.disciplineIncidentReference.schoolId",
        "behavior_descriptor":  "$.behaviorDescriptor",
    },
    "student_discipline_incident_non_offender_associations": {
        "student_unique_id":    "$.studentReference.studentUniqueId",
        "incident_identifier":  "$.disciplineIncidentReference.incidentIdentifier",
        "school_id":            "$.disciplineIncidentReference.schoolId",
    },
    "restraint_events": {
        "restraint_event_identifier": "$.restraintEventIdentifier",
        "student_unique_id":    "$.studentReference.studentUniqueId",
        "school_id":            "$.schoolReference.schoolId",
        "event_date":           "$.eventDate",
    },

    # ----- TPDM extension -----
    "candidates": {
        "candidate_identifier": "$.candidateIdentifier",
        "first_name":           "$.firstName",
        "last_name":            "$.lastSurname",
        "birth_date":           "$.birthDate",
        "sex_descriptor":       "$.sexDescriptor",
        "person_id":            "$.personReference.personId",
    },
    "educator_preparation_programs": {
        "program_name":         "$.programName",
        "program_type_descriptor": "$.programTypeDescriptor",
        "education_organization_id": "$.educationOrganizationReference.educationOrganizationId",
    },
    # TPDM 7.3: the pathway descriptor is `eppProgramPathwayDescriptor`.
    "candidate_educator_preparation_program_associations": {
        "candidate_identifier": "$.candidateReference.candidateIdentifier",
        "program_name":         "$.educatorPreparationProgramReference.programName",
        "program_type_descriptor": "$.educatorPreparationProgramReference.programTypeDescriptor",
        "education_organization_id": "$.educatorPreparationProgramReference.educationOrganizationId",
        "begin_date":           "$.beginDate",
        "end_date":             "$.endDate",
        "ep_program_pathway_descriptor": "$.eppProgramPathwayDescriptor",
    },
}
