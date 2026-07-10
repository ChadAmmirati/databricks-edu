"""
Deterministic synthetic Ed-Fi data generator.

Payload shapes are validated against the Ed-Fi 7.3 (Data Standard 5.x) OpenAPI
spec — references are nested exactly as the real API returns them, every record
carries a stable GUID `id` like the real API, and EVERY endpoint that exists on
a 7.3 ODS produces non-empty data. Endpoints that do NOT exist on 7.3
('parents', 'studentParentAssociations', 'studentDisciplineIncidentAssociations')
are OMITTED from the dict entirely, so synthetic runs exercise the same
404-skip path a real ODS triggers.

The special key `_deletes` maps resource -> tombstones [{id, changeVersion}]
for records that ARE present in the generated data, so delete propagation
through Silver is exercised end-to-end on every synthetic run.

Use this when a real Ed-Fi ODS or the public sandbox isn't reachable —
e.g. CI, offline demos, or first-deploy smoke runs.
"""
from __future__ import annotations

import hashlib
import random
import re
from datetime import date, timedelta
from typing import Dict, List

_EDFI = "uri://ed-fi.org"
_TPDM = "uri://tpdm.ed-fi.org"

# Records every (namespace_base, descriptor_kind) -> {codeValues} produced by
# `_d` during a generate run, so we emit a matching descriptor record for every
# descriptor value the data actually uses — no unmatched codes, no invented
# values. Reset at the top of generate_synthetic_dataset().
_DESCRIPTOR_USE: dict = {}


def _uid(*parts) -> str:
    """Stable GUID-shaped id derived from the record's natural key."""
    h = hashlib.sha1("|".join(str(p) for p in parts).encode("utf-8")).hexdigest()
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


def _d(kind: str, value: str, base: str = _EDFI) -> str:
    """Build an Ed-Fi descriptor URI and record (base, kind, value) so a matching
    descriptor record is emitted for it (see _emit_descriptors)."""
    _DESCRIPTOR_USE.setdefault((base, kind), set()).add(value)
    return f"{base}/{kind}#{value}"


def _descriptor_resource_name(base: str, kind: str) -> str:
    """'SexDescriptor' -> 'sex_descriptors'. tpdm kinds get a 'tpdm_' prefix so
    same-named ed-fi/tpdm descriptors don't collide as manifest resources."""
    snake = re.sub(r"(?<!^)(?=[A-Z])", "_", kind).lower()
    prefix = "tpdm_" if base == _TPDM else ""
    return f"{prefix}{snake}s"


def _emit_descriptors(out: "Dict[str, List[dict]]") -> None:
    """Emit one descriptor record per (namespace, codeValue) recorded by `_d`, so
    Silver's `_descriptors` lookup covers every code the data uses. In synthetic
    mode shortDescription == codeValue (a synthetic ODS legitimately defines its
    own codes); on a real ODS these come from the API's *Descriptors endpoints."""
    for (base, kind), values in sorted(_DESCRIPTOR_USE.items()):
        out[_descriptor_resource_name(base, kind)] = [
            {
                "id": _uid("descriptor", base, kind, v),
                "namespace": f"{base}/{kind}",
                "codeValue": v,
                "shortDescription": v,
                "description": v,
            }
            for v in sorted(values)
        ]


def generate_synthetic_dataset(
    *,
    n_schools: int = 5,
    n_students_per_school: int = 60,
    n_staff_per_school: int = 12,
    school_year: int = 2026,
    seed: int = 42,
) -> Dict[str, List[dict]]:
    rng = random.Random(seed)
    _DESCRIPTOR_USE.clear()
    out: Dict[str, List[dict]] = {}
    yr0, yr1 = school_year - 1, school_year
    session_name = f"{yr0}-{yr1} Year"

    # ---- Organizations ----
    out["state_education_agencies"] = [{
        "id": _uid("sea", 100),
        "stateEducationAgencyId": 100,
        "nameOfInstitution": "Agency of Education",
        "categories": [{"educationOrganizationCategoryDescriptor": _d("EducationOrganizationCategoryDescriptor", "State Education Agency")}],
    }]
    out["education_service_centers"] = [{
        "id": _uid("esc", 500),
        "educationServiceCenterId": 500,
        "nameOfInstitution": "Central Valley Service Center",
        "categories": [{"educationOrganizationCategoryDescriptor": _d("EducationOrganizationCategoryDescriptor", "Education Service Center")}],
    }]
    out["local_education_agencies"] = [{
        "id": _uid("lea", 1000),
        "localEducationAgencyId": 1000,
        "nameOfInstitution": "Demo School District",
        "localEducationAgencyCategoryDescriptor": _d("LocalEducationAgencyCategoryDescriptor", "Independent"),
        "categories": [{"educationOrganizationCategoryDescriptor": _d("EducationOrganizationCategoryDescriptor", "Local Education Agency")}],
    }]
    out["post_secondary_institutions"] = [{
        "id": _uid("psi", 9000),
        "postSecondaryInstitutionId": 9000,
        "nameOfInstitution": "Demo Community College",
        "categories": [{"educationOrganizationCategoryDescriptor": _d("EducationOrganizationCategoryDescriptor", "Post Secondary Institution")}],
    }]

    schools = []
    for i in range(n_schools):
        sid = 2000 + i
        schools.append({
            "id": _uid("school", sid),
            "schoolId": sid,
            "nameOfInstitution": f"Demo School {i+1}",
            "localEducationAgencyReference": {"localEducationAgencyId": 1000},
            "gradeLevels": [{"gradeLevelDescriptor": _d("GradeLevelDescriptor", g)} for g in _grade_band_for_school(i)],
            "schoolCategories": [{"schoolCategoryDescriptor": _d("SchoolCategoryDescriptor", "Elementary School")}],
            "addresses": [{
                "addressTypeDescriptor": _d("AddressTypeDescriptor", "Physical"),
                "city": "Springfield",
                "stateAbbreviationDescriptor": _d("StateAbbreviationDescriptor", "DS"),
                "postalCode": "05601", "streetNumberName": f"{100+i} School St",
            }],
        })
    out["schools"] = schools

    out["education_organization_networks"] = [{
        "id": _uid("eon", 7000),
        "educationOrganizationNetworkId": 7000,
        "nameOfInstitution": "Rural Schools Network",
        "networkPurposeDescriptor": _d("NetworkPurposeDescriptor", "Shared Services"),
        "categories": [{"educationOrganizationCategoryDescriptor": _d("EducationOrganizationCategoryDescriptor", "Education Organization Network")}],
    }]
    out["education_organization_network_associations"] = [{
        "id": _uid("eona", 7000, s["schoolId"]),
        "educationOrganizationNetworkReference": {"educationOrganizationNetworkId": 7000},
        "memberEducationOrganizationReference": {"educationOrganizationId": s["schoolId"]},
        "beginDate": f"{yr0}-07-01",
    } for s in schools[:3]]

    # ---- Calendars, sessions, periods ----
    sessions, grading_periods, calendars, calendar_dates = [], [], [], []
    bell_schedules, class_periods, locations = [], [], []
    quarters = [("Quarter 1", 1, f"{yr0}-08-25", f"{yr0}-10-31"), ("Quarter 2", 2, f"{yr0}-11-01", f"{yr1}-01-15"),
                ("Quarter 3", 3, f"{yr1}-01-16", f"{yr1}-03-31"), ("Quarter 4", 4, f"{yr1}-04-01", f"{yr1}-06-15")]
    for s in schools:
        sid = s["schoolId"]
        sessions.append({
            "id": _uid("session", sid, school_year, session_name),
            "schoolReference": {"schoolId": sid},
            "schoolYearTypeReference": {"schoolYear": school_year},
            "sessionName": session_name,
            "beginDate": f"{yr0}-08-25", "endDate": f"{yr1}-06-15",
            "totalInstructionalDays": 180,
            "termDescriptor": _d("TermDescriptor", "Year Round"),
        })
        for qname, qseq, qbegin, qend in quarters:
            grading_periods.append({
                "id": _uid("gp", sid, school_year, qname),
                "gradingPeriodDescriptor": _d("GradingPeriodDescriptor", "First Nine Weeks" if qseq == 1 else f"{['','First','Second','Third','Fourth'][qseq]} Nine Weeks"),
                "gradingPeriodName": qname,
                "schoolReference": {"schoolId": sid},
                "schoolYearTypeReference": {"schoolYear": school_year},
                "periodSequence": qseq,
                "beginDate": qbegin, "endDate": qend,
                "totalInstructionalDays": 45,
            })
        cal_code = f"CAL-{sid}"
        calendars.append({
            "id": _uid("cal", sid, school_year, cal_code),
            "calendarCode": cal_code,
            "schoolReference": {"schoolId": sid},
            "schoolYearTypeReference": {"schoolYear": school_year},
            "calendarTypeDescriptor": _d("CalendarTypeDescriptor", "Student Specific"),
        })
        d, added = date(yr0, 8, 25), 0
        while added < 180:
            if d.weekday() < 5:
                calendar_dates.append({
                    "id": _uid("cd", sid, school_year, cal_code, d.isoformat()),
                    "calendarReference": {"calendarCode": cal_code, "schoolId": sid, "schoolYear": school_year},
                    "date": d.isoformat(),
                    "calendarEvents": [{"calendarEventDescriptor": _d("CalendarEventDescriptor", "Instructional day")}],
                })
                added += 1
            d += timedelta(days=1)
        bell_schedules.append({
            "id": _uid("bell", sid),
            "bellScheduleName": f"Standard Day {sid}",
            "schoolReference": {"schoolId": sid},
            "classPeriods": [{"classPeriodReference": {"classPeriodName": f"Period {p}", "schoolId": sid}} for p in range(1, 3)],
            "totalInstructionalTime": 360,
        })
        for p in range(1, 3):
            class_periods.append({
                "id": _uid("cp", sid, p),
                "classPeriodName": f"Period {p}",
                "schoolReference": {"schoolId": sid},
                "meetingTimes": [{"startTime": f"0{7+p}:30:00", "endTime": f"0{8+p}:20:00"}],
            })
        for room in ("101", "102"):
            locations.append({
                "id": _uid("loc", sid, room),
                "classroomIdentificationCode": f"RM-{room}",
                "schoolReference": {"schoolId": sid},
                "maximumNumberOfSeats": 28,
            })
    out["sessions"] = sessions
    out["grading_periods"] = grading_periods
    out["calendars"] = calendars
    out["calendar_dates"] = calendar_dates
    out["bell_schedules"] = bell_schedules
    out["class_periods"] = class_periods
    out["locations"] = locations

    out["graduation_plans"] = [{
        "id": _uid("gradplan", 1000, school_year),
        "graduationPlanTypeDescriptor": _d("GraduationPlanTypeDescriptor", "Recommended"),
        "educationOrganizationReference": {"educationOrganizationId": 1000},
        "graduationSchoolYearTypeReference": {"schoolYear": school_year},
        "totalRequiredCredits": 24.0,
    }]

    # ---- Courses, offerings, sections ----
    courses, course_offerings, sections = [], [], []
    course_catalog = [
        ("ENG101", "English Language Arts", "English Language Arts"),
        ("MAT101", "Mathematics", "Mathematics"),
        ("SCI101", "Science", "Science"),
        ("SOC101", "Social Studies", "Social Studies"),
        ("PE101", "Physical Education", "Physical, Health, and Safety Education"),
    ]
    for s in schools:
        sid = s["schoolId"]
        for code, title, area in course_catalog:
            courses.append({
                "id": _uid("course", code, sid),
                "courseCode": code,
                "educationOrganizationReference": {"educationOrganizationId": sid},
                "courseTitle": title,
                "academicSubjects": [{"academicSubjectDescriptor": _d("AcademicSubjectDescriptor", area)}],
                "numberOfParts": 1,
            })
            course_offerings.append({
                "id": _uid("co", code, sid, school_year),
                "localCourseCode": code,
                "schoolReference": {"schoolId": sid},
                "sessionReference": {"schoolId": sid, "schoolYear": school_year, "sessionName": session_name},
                "courseReference": {"courseCode": code, "educationOrganizationId": sid},
                "localCourseTitle": title,
            })
            section_rec = {
                "id": _uid("section", code, sid, school_year, f"{code}-A"),
                "sectionIdentifier": f"{code}-A",
                "courseOfferingReference": {
                    "localCourseCode": code, "schoolId": sid,
                    "schoolYear": school_year, "sessionName": session_name,
                },
                "locationReference": {"classroomIdentificationCode": "RM-101", "schoolId": sid},
                "availableCredits": 1,
                "educationalEnvironmentDescriptor": _d("EducationalEnvironmentDescriptor", "Classroom"),
            }
            if code == "PE101":
                section_rec["programs"] = [{"programReference": {
                    "programName": "Career and Technical Education",
                    "programTypeDescriptor": _d("ProgramTypeDescriptor", "Career and Technical Education"),
                    "educationOrganizationId": 1000,
                }}]
            sections.append(section_rec)
    out["courses"] = courses
    out["course_offerings"] = course_offerings
    out["sections"] = sections

    # ---- Programs ----
    program_defs = [
        ("Title I Part A", "Title I Part A"),
        ("Special Education", "Special Education"),
        ("English as a Second Language (ESL)", "English as a Second Language (ESL)"),
        ("Homeless", "Homeless"),
        ("Career and Technical Education", "Career and Technical Education"),
        ("Migrant Education", "Migrant Education"),
        ("School Food Service", "School Food Service"),
    ]
    out["programs"] = [{
        "id": _uid("program", name, 1000),
        "programName": name,
        "programTypeDescriptor": _d("ProgramTypeDescriptor", ptype),
        "educationOrganizationReference": {"educationOrganizationId": 1000},
    } for name, ptype in program_defs]

    def program_ref(name):
        ptype = dict(program_defs)[name]
        return {"programName": name, "programTypeDescriptor": _d("ProgramTypeDescriptor", ptype),
                "educationOrganizationId": 1000}

    out["program_evaluations"] = [{
        "id": _uid("pe", "Title I Annual Review"),
        "programEvaluationTitle": "Title I Annual Review",
        "programEvaluationTypeDescriptor": _d("ProgramEvaluationTypeDescriptor", "Formative Assessment"),
        "programEvaluationPeriodDescriptor": _d("ProgramEvaluationPeriodDescriptor", "End of Year"),
        "programReference": program_ref("Title I Part A"),
    }]

    # ---- Cohorts ----
    cohorts, student_cohort_assoc = [], []
    for s in schools:
        cohorts.append({
            "id": _uid("cohort", s["schoolId"], "MTSS"),
            "cohortIdentifier": f"MTSS-{s['schoolId']}",
            "educationOrganizationReference": {"educationOrganizationId": s["schoolId"]},
            "cohortDescription": "Multi-tiered support cohort",
            "cohortTypeDescriptor": _d("CohortTypeDescriptor", "Academic Intervention"),
        })
    out["cohorts"] = cohorts

    # ---- People / staff / contacts ----
    first_names = ["Avery", "Bailey", "Cameron", "Dakota", "Emerson", "Finley", "Hayden", "Indigo"]
    last_names = ["Allen", "Baker", "Carter", "Diaz", "Evans", "Foster", "Green", "Hayes"]
    staff_first = ["Alex", "Sam", "Jordan", "Taylor", "Casey", "Riley", "Morgan", "Quinn"]
    sex_choices = ["Male", "Female", "Not Selected"]
    race_choices = ["White", "Black - African American", "Asian", "American Indian - Alaska Native"]
    grade_choices = ["First grade", "Second grade", "Third grade", "Fourth grade", "Fifth grade",
                     "Sixth grade", "Seventh grade", "Eighth grade", "Ninth grade", "Tenth grade",
                     "Eleventh grade", "Twelfth grade"]

    out["people"] = [{
        "id": _uid("person", f"P{j:04d}"),
        "personId": f"P{j:04d}",
        "sourceSystemDescriptor": _d("SourceSystemDescriptor", "District"),
    } for j in range(20)]

    staffs, staff_school_assoc, staff_section_assoc = [], [], []
    staff_eo_assignment, staff_eo_employment = [], []
    credentials = []
    students, student_school_assoc, student_section_assoc, student_eo_assoc = [], [], [], []
    student_eo_resp_assoc, contacts, student_contact_assoc = [], [], []
    student_school_attend, student_section_attend, grades = [], [], []
    student_assessments, student_academic_records, course_transcripts = [], [], []
    discipline_incidents, discipline_actions = [], []
    sdi_behavior_assoc, sdi_non_offender_assoc, restraint_events = [], [], []
    program_assocs: Dict[str, List[dict]] = {
        "student_program_associations": [],
        "student_special_education_program_associations": [],
        "student_homeless_program_associations": [],
        "student_language_instruction_program_associations": [],
        "student_title_i_part_a_program_associations": [],
        "student_cte_program_associations": [],
        "student_migrant_education_program_associations": [],
        "student_school_food_service_program_association": [],
    }
    sped_eligibility = []
    student_program_evaluations = []

    for s in schools:
        sid = s["schoolId"]
        # ---- staff ----
        for j in range(n_staff_per_school):
            staff_id = f"ST{sid}-{j:03d}"
            staffs.append({
                "id": _uid("staff", staff_id),
                "staffUniqueId": staff_id,
                "firstName": rng.choice(staff_first),
                "lastSurname": rng.choice(last_names),
                "birthDate": "1985-04-12",
                "highestCompletedLevelOfEducationDescriptor": _d("LevelOfEducationDescriptor", "Master's"),
            })
            staff_school_assoc.append({
                "id": _uid("ssa", staff_id, sid),
                "staffReference": {"staffUniqueId": staff_id},
                "schoolReference": {"schoolId": sid},
                "programAssignmentDescriptor": _d("ProgramAssignmentDescriptor", "Regular Education"),
            })
            assigned = rng.choice([c[0] for c in course_catalog])
            staff_section_assoc.append({
                "id": _uid("ssec", staff_id, assigned, sid),
                "staffReference": {"staffUniqueId": staff_id},
                "sectionReference": {
                    "localCourseCode": assigned, "schoolId": sid, "schoolYear": school_year,
                    "sessionName": session_name, "sectionIdentifier": f"{assigned}-A",
                },
                "classroomPositionDescriptor": _d("ClassroomPositionDescriptor", "Teacher of Record"),
            })
            staff_eo_assignment.append({
                "id": _uid("seoa", staff_id, sid),
                "staffReference": {"staffUniqueId": staff_id},
                "educationOrganizationReference": {"educationOrganizationId": sid},
                "staffClassificationDescriptor": _d("StaffClassificationDescriptor", "Teacher"),
                "beginDate": f"{yr0}-08-15",
                "positionTitle": "Classroom Teacher",
            })
            staff_eo_employment.append({
                "id": _uid("seoe", staff_id, sid),
                "staffReference": {"staffUniqueId": staff_id},
                "educationOrganizationReference": {"educationOrganizationId": sid},
                "employmentStatusDescriptor": _d("EmploymentStatusDescriptor", "Tenured or permanent"),
                "hireDate": f"{yr0 - rng.randint(1, 10)}-08-01",
            })
            if j == 0:
                credentials.append({
                    "id": _uid("cred", staff_id),
                    "credentialIdentifier": f"CRED-{staff_id}",
                    "stateOfIssueStateAbbreviationDescriptor": _d("StateAbbreviationDescriptor", "DS"),
                    "credentialTypeDescriptor": _d("CredentialTypeDescriptor", "Certification"),
                    "teachingCredentialDescriptor": _d("TeachingCredentialDescriptor", "Professional"),
                    "namespace": _EDFI,
                })

        # ---- students ----
        for j in range(n_students_per_school):
            stu_id = f"STU{sid}-{j:04d}"
            grade = rng.choice(grade_choices)
            students.append({
                "id": _uid("student", stu_id),
                "studentUniqueId": stu_id,
                "firstName": rng.choice(first_names),
                "middleName": rng.choice(["Lee", "Ray", "Sky", ""]) or None,
                "lastSurname": rng.choice(last_names),
                "birthDate": f"{school_year - rng.randint(6, 18)}-0{rng.randint(1, 9)}-1{rng.randint(0, 8)}",
                "birthCity": "Springfield",
                "birthStateAbbreviationDescriptor": _d("StateAbbreviationDescriptor", "DS"),
                "birthSexDescriptor": _d("SexDescriptor", rng.choice(sex_choices)),
            })
            entry_date = f"{yr0}-08-25"
            student_school_assoc.append({
                "id": _uid("enroll", stu_id, sid, entry_date),
                "studentReference": {"studentUniqueId": stu_id},
                "schoolReference": {"schoolId": sid},
                "entryDate": entry_date,
                "entryGradeLevelDescriptor": _d("GradeLevelDescriptor", grade),
                "schoolYearTypeReference": {"schoolYear": school_year},
            })
            # race profile: most single-race; ~15% multi-race (-> 'Multiple'),
            # ~10% no race (-> 'Unknown' unless Hispanic) — exercises the rollup.
            _roll = rng.random()
            if _roll < 0.10:
                _races = []
            elif _roll < 0.25:
                _races = [{"raceDescriptor": _d("RaceDescriptor", r)} for r in rng.sample(race_choices, 2)]
            else:
                _races = [{"raceDescriptor": _d("RaceDescriptor", rng.choice(race_choices))}]
            student_eo_assoc.append({
                "id": _uid("seoassoc", stu_id, sid),
                "studentReference": {"studentUniqueId": stu_id},
                "educationOrganizationReference": {"educationOrganizationId": sid},
                "races": _races,
                "hispanicLatinoEthnicity": rng.random() < 0.18,
                "sexDescriptor": _d("SexDescriptor", rng.choice(sex_choices)),
                "limitedEnglishProficiencyDescriptor": _d("LimitedEnglishProficiencyDescriptor", "NotLimited"),
            })
            # Real ODSes have a second LEA-level association per student — the
            # demographics join must tolerate it (regression for dim_student fan-out).
            student_eo_assoc.append({
                "id": _uid("seoassoc", stu_id, 1000),
                "studentReference": {"studentUniqueId": stu_id},
                "educationOrganizationReference": {"educationOrganizationId": 1000},
                "sexDescriptor": _d("SexDescriptor", rng.choice(sex_choices)),
            })
            student_eo_resp_assoc.append({
                "id": _uid("seora", stu_id, 1000),
                "studentReference": {"studentUniqueId": stu_id},
                "educationOrganizationReference": {"educationOrganizationId": 1000},
                "responsibilityDescriptor": _d("ResponsibilityDescriptor", "Accountability"),
                "beginDate": entry_date,
            })
            # contacts (DS 5.x replacement for parents)
            contact_id = f"CT{sid}-{j:04d}"
            contacts.append({
                "id": _uid("contact", contact_id),
                "contactUniqueId": contact_id,
                "firstName": rng.choice(first_names),
                "lastSurname": rng.choice(last_names),
                "sexDescriptor": _d("SexDescriptor", rng.choice(sex_choices[:2])),
            })
            student_contact_assoc.append({
                "id": _uid("sca", stu_id, contact_id),
                "studentReference": {"studentUniqueId": stu_id},
                "contactReference": {"contactUniqueId": contact_id},
                "relationDescriptor": _d("RelationDescriptor", rng.choice(["Mother", "Father", "Guardian"])),
                "primaryContactStatus": True,
                "emergencyContactStatus": True,
            })
            if j < 10:
                coh_rec = {
                    "id": _uid("scoh", stu_id, sid),
                    "studentReference": {"studentUniqueId": stu_id},
                    "cohortReference": {"cohortIdentifier": f"MTSS-{sid}", "educationOrganizationId": sid},
                    "beginDate": entry_date,
                }
                if j < 2:  # a few exited cohort members so end_date flows through
                    coh_rec["endDate"] = f"{yr1}-02-01"
                student_cohort_assoc.append(coh_rec)

            # enrollment in core sections + grades
            for code, _, _ in course_catalog:
                section_ref = {
                    "localCourseCode": code, "schoolId": sid, "schoolYear": school_year,
                    "sessionName": session_name, "sectionIdentifier": f"{code}-A",
                }
                ssa_rec = {
                    "id": _uid("ssecassoc", stu_id, code, sid),
                    "studentReference": {"studentUniqueId": stu_id},
                    "sectionReference": section_ref,
                    "beginDate": entry_date,
                }
                if j < 3:  # a few completed enrollments so end_date flows through
                    ssa_rec["endDate"] = f"{yr1}-01-15"
                student_section_assoc.append(ssa_rec)
                grade_rec = {
                    "id": _uid("grade", stu_id, code, sid, "Q4"),
                    "gradeTypeDescriptor": _d("GradeTypeDescriptor", "Final"),
                    "studentSectionAssociationReference": {**section_ref, "studentUniqueId": stu_id, "beginDate": entry_date},
                    "gradingPeriodReference": {
                        "gradingPeriodDescriptor": _d("GradingPeriodDescriptor", "Fourth Nine Weeks"),
                        "gradingPeriodName": "Quarter 4",
                        "schoolId": sid, "schoolYear": school_year,
                    },
                    "numericGradeEarned": round(60 + rng.random() * 40, 2),
                    "letterGradeEarned": rng.choice(["A", "B", "C", "D"]),
                }
                if code in ("ENG101", "MAT101") and j < 10:
                    grade_rec["learningStandardGrades"] = [{
                        "learningStandardReference": {"learningStandardId": "MA.10.1" if code == "MAT101" else "EN.10.1"},
                        "performanceBaseConversionDescriptor": _d("PerformanceBaseConversionDescriptor", "Proficient"),
                        "diagnosticStatement": "synthetic",
                    }]
                grades.append(grade_rec)

            # attendance — school-level daily over ~40 instructional (weekday) days.
            # ~20% of students follow a chronically-absent profile (>10% of days
            # absent); the rest attend regularly — so the chronic-absence flag is
            # exercised with a realistic mix (some True, some False), never hollow.
            is_chronic_profile = rng.random() < 0.20
            absent_prob = 0.22 if is_chronic_profile else 0.03
            _d2 = date(yr0, 9, 1)
            _recorded = 0
            while _recorded < 40:
                if _d2.weekday() < 5:  # weekdays only
                    if rng.random() < absent_prob:
                        cat = rng.choice(["Excused Absence", "Unexcused Absence"])
                    elif rng.random() < 0.04:
                        cat = "Tardy"  # present (is_absent stays False)
                    else:
                        cat = "In Attendance"
                    student_school_attend.append({
                        "id": _uid("att", stu_id, sid, _d2.isoformat()),
                        "attendanceEventCategoryDescriptor": _d("AttendanceEventCategoryDescriptor", cat),
                        "eventDate": _d2.isoformat(),
                        "schoolReference": {"schoolId": sid},
                        "sessionReference": {"schoolId": sid, "schoolYear": school_year, "sessionName": session_name},
                        "studentReference": {"studentUniqueId": stu_id},
                        "eventDuration": 1.0,
                    })
                    _recorded += 1
                _d2 += timedelta(days=1)
            for offset_days in range(3):
                d2 = date(yr0, 9, 1) + timedelta(days=offset_days)
                student_section_attend.append({
                    "id": _uid("satt", stu_id, "ENG101", sid, d2.isoformat()),
                    "attendanceEventCategoryDescriptor": _d("AttendanceEventCategoryDescriptor", "In Attendance"),
                    "eventDate": d2.isoformat(),
                    "sectionReference": {
                        "localCourseCode": "ENG101", "schoolId": sid, "schoolYear": school_year,
                        "sessionName": session_name, "sectionIdentifier": "ENG101-A",
                    },
                    "studentReference": {"studentUniqueId": stu_id},
                })

            # one state assessment per student, with objective scores
            student_assessments.append({
                "id": _uid("sa", stu_id, "DEMO-MATH"),
                "studentAssessmentIdentifier": _uid("said", stu_id, "MATH")[:16],
                "assessmentReference": {"assessmentIdentifier": "DEMO-MATH", "namespace": f"{_EDFI}/Assessment"},
                "studentReference": {"studentUniqueId": stu_id},
                "administrationDate": f"{yr1}-04-15",
                "scoreResults": [{
                    "assessmentReportingMethodDescriptor": _d("AssessmentReportingMethodDescriptor", "Scale score"),
                    "resultDatatypeTypeDescriptor": _d("ResultDatatypeTypeDescriptor", "Integer"),
                    "result": str(rng.randint(200, 600)),
                }],
                "studentObjectiveAssessments": [{
                    "objectiveAssessmentReference": {
                        "assessmentIdentifier": "DEMO-MATH",
                        "namespace": f"{_EDFI}/Assessment",
                        "identificationCode": code,
                    },
                    "scoreResults": [{
                        "assessmentReportingMethodDescriptor": _d("AssessmentReportingMethodDescriptor", "Scale score"),
                        "result": str(rng.randint(20, 100)),
                    }],
                } for code in ("ALG", "GEO", "STAT")],
            })

    out["staffs"] = staffs
    out["staff_school_associations"] = staff_school_assoc
    out["staff_section_associations"] = staff_section_assoc
    out["staff_education_organization_assignment_associations"] = staff_eo_assignment
    out["staff_education_organization_employment_associations"] = staff_eo_employment
    out["credentials"] = credentials
    out["students"] = students
    out["contacts"] = contacts
    out["student_contact_associations"] = student_contact_assoc
    out["student_school_associations"] = student_school_assoc
    out["student_section_associations"] = student_section_assoc
    out["student_education_organization_associations"] = student_eo_assoc
    out["student_education_organization_responsibility_associations"] = student_eo_resp_assoc
    out["student_cohort_associations"] = student_cohort_assoc
    out["student_school_attendance_events"] = student_school_attend
    out["student_section_attendance_events"] = student_section_attend
    out["grades"] = grades
    out["student_assessments"] = student_assessments

    # NOTE: 'parents', 'student_parent_associations', and
    # 'student_discipline_incident_associations' are deliberately ABSENT —
    # they do not exist on an Ed-Fi 7.3 / DS 5.x ODS. Their omission exercises
    # the optional-endpoint skip path. Gold sources parents from contacts and
    # incident participation from the behavior/non-offender associations.

    # ---- assessments dims ----
    out["assessments"] = [{
        "id": _uid("assessment", "DEMO-MATH"),
        "assessmentIdentifier": "DEMO-MATH",
        "namespace": f"{_EDFI}/Assessment",
        "assessmentTitle": "Demo Math Assessment",
        "assessmentCategoryDescriptor": _d("AssessmentCategoryDescriptor", "State summative assessment 4th grade"),
        "academicSubjects": [{"academicSubjectDescriptor": _d("AcademicSubjectDescriptor", "Mathematics")}],
    }]
    out["objective_assessments"] = [{
        "id": _uid("oa", "DEMO-MATH", code),
        "identificationCode": code,
        "assessmentReference": {"assessmentIdentifier": "DEMO-MATH", "namespace": f"{_EDFI}/Assessment"},
        "description": desc,
        "maxRawScore": 100,
    } for code, desc in [("ALG", "Algebra strand"), ("GEO", "Geometry strand"), ("STAT", "Statistics strand")]]
    out["learning_standards"] = [{
        "id": _uid("ls", ls_id),
        "learningStandardId": ls_id,
        "description": desc,
        "academicSubjects": [{"academicSubjectDescriptor": _d("AcademicSubjectDescriptor", subj)}],
    } for ls_id, desc, subj in [
        ("MA.10.1", "10th-grade Algebra I standard 1", "Mathematics"),
        ("EN.10.1", "10th-grade ELA reading-comprehension standard 1", "English Language Arts"),
    ]]

    # ---- academic records (DS 5.x: GPA lives in gradePointAverages[]) ----
    senior_subset = students[: max(1, len(students) // 4)]
    for stu in senior_subset:
        term = _d("TermDescriptor", "Spring Semester")
        cum_gpa = round(2.0 + rng.random() * 2.0, 2)
        student_academic_records.append({
            "id": _uid("sar", stu["studentUniqueId"], school_year),
            "termDescriptor": term,
            "educationOrganizationReference": {"educationOrganizationId": schools[0]["schoolId"]},
            "schoolYearTypeReference": {"schoolYear": school_year},
            "studentReference": {"studentUniqueId": stu["studentUniqueId"]},
            "cumulativeAttemptedCredits": round(22 + rng.random() * 4, 2),
            "cumulativeEarnedCredits": round(20 + rng.random() * 4, 2),
            "gradePointAverages": [
                {"gradePointAverageTypeDescriptor": _d("GradePointAverageTypeDescriptor", "Unweighted"),
                 "isCumulative": True, "gradePointAverageValue": cum_gpa, "maxGradePointAverageValue": 4.0},
                {"gradePointAverageTypeDescriptor": _d("GradePointAverageTypeDescriptor", "Unweighted"),
                 "isCumulative": False, "gradePointAverageValue": round(2.0 + rng.random() * 2.0, 2),
                 "maxGradePointAverageValue": 4.0},
            ],
            "diplomas": [{
                "diplomaAwardDate": f"{yr1}-06-15",
                "diplomaTypeDescriptor": _d("DiplomaTypeDescriptor", "Regular diploma"),
                "diplomaLevelDescriptor": _d("DiplomaLevelDescriptor", "Standard"),
            }],
        })
        for code, title, _ in course_catalog:
            course_transcripts.append({
                "id": _uid("ct", stu["studentUniqueId"], code, school_year),
                "courseAttemptResultDescriptor": _d("CourseAttemptResultDescriptor", "Pass"),
                "courseReference": {"courseCode": code, "educationOrganizationId": schools[0]["schoolId"]},
                "studentAcademicRecordReference": {
                    "studentUniqueId": stu["studentUniqueId"], "schoolYear": school_year,
                    "termDescriptor": term, "educationOrganizationId": schools[0]["schoolId"],
                },
                "finalLetterGradeEarned": rng.choice(["A", "B", "C"]),
                "finalNumericGradeEarned": round(70 + rng.random() * 30, 2),
                "attemptedCredits": 1.0,
                "earnedCredits": 1.0,
            })
    out["student_academic_records"] = student_academic_records
    out["course_transcripts"] = course_transcripts

    # ---- program associations ----
    def add_prog_assoc(bucket, students_slice, prog_name, extra=None):
        for stu in students_slice:
            rec = {
                "id": _uid(bucket, stu["studentUniqueId"], prog_name),
                "studentReference": {"studentUniqueId": stu["studentUniqueId"]},
                "educationOrganizationReference": {"educationOrganizationId": 1000},
                "programReference": program_ref(prog_name),
                "beginDate": f"{yr0}-09-01",
            }
            # ~30% exit mid-year: end_date before year-end, so they count for
            # 'annual' (served this year) but not 'active' (open at year-end) —
            # exercises the program active/annual distinction with real values.
            if rng.random() < 0.30:
                rec["endDate"] = f"{yr1}-02-15"
            if extra:
                rec.update(extra)
            program_assocs[bucket].append(rec)

    n = len(students)
    add_prog_assoc("student_program_associations", students[: n // 5], "Title I Part A",
                   {"services": [{
                       "serviceDescriptor": _d("ServiceDescriptor", "Pull-out reading support"),
                       "primaryIndicator": True,
                       "serviceBeginDate": f"{yr0}-09-01", "serviceEndDate": f"{yr1}-06-15",
                   }]})
    add_prog_assoc("student_special_education_program_associations", students[: n // 8], "Special Education",
                   {"ideaEligibility": True})
    add_prog_assoc("student_homeless_program_associations", students[: n // 20], "Homeless")
    add_prog_assoc("student_language_instruction_program_associations", students[: n // 10],
                   "English as a Second Language (ESL)")
    add_prog_assoc("student_title_i_part_a_program_associations", students[: n // 5], "Title I Part A")
    add_prog_assoc("student_cte_program_associations", students[: n // 10], "Career and Technical Education")
    add_prog_assoc("student_migrant_education_program_associations", students[: n // 30], "Migrant Education")
    add_prog_assoc("student_school_food_service_program_association", students[: n // 3], "School Food Service")
    out.update(program_assocs)

    for stu in students[: n // 8]:
        sped_eligibility.append({
            "id": _uid("spedelig", stu["studentUniqueId"]),
            "studentReference": {"studentUniqueId": stu["studentUniqueId"]},
            "educationOrganizationReference": {"educationOrganizationId": 1000},
            "programReference": program_ref("Special Education"),
            "consentToEvaluationReceivedDate": f"{yr0}-09-15",
            "ideaIndicator": True,
        })
    out["student_special_education_program_eligibility_associations"] = sped_eligibility

    for stu in students[: n // 5][:20]:
        student_program_evaluations.append({
            "id": _uid("spe", stu["studentUniqueId"]),
            "evaluationDate": f"{yr1}-05-15",
            "programEvaluationReference": {
                "programEvaluationTitle": "Title I Annual Review",
                "programEvaluationTypeDescriptor": _d("ProgramEvaluationTypeDescriptor", "Formative Assessment"),
                "programEvaluationPeriodDescriptor": _d("ProgramEvaluationPeriodDescriptor", "End of Year"),
                "programName": "Title I Part A",
                "programTypeDescriptor": _d("ProgramTypeDescriptor", "Title I Part A"),
                "programEducationOrganizationId": 1000,
            },
            "studentReference": {"studentUniqueId": stu["studentUniqueId"]},
            "summaryEvaluationNumericRating": round(1 + rng.random() * 4, 1),
        })
    out["student_program_evaluations"] = student_program_evaluations

    # ---- discipline (DS 5.x: participation via behavior/non-offender assocs) ----
    for s in schools[:2]:
        sid = s["schoolId"]
        incident_id = f"INC-{sid}-001"
        discipline_incidents.append({
            "id": _uid("inc", sid, incident_id),
            "incidentIdentifier": incident_id,
            "schoolReference": {"schoolId": sid},
            "incidentDate": f"{yr0}-10-15",
            "incidentDescription": "Synthetic demo incident",
        })
        offender = students[rng.randint(0, len(students) - 1)]
        witness = students[rng.randint(0, len(students) - 1)]
        sdi_behavior_assoc.append({
            "id": _uid("sdib", offender["studentUniqueId"], incident_id),
            "behaviorDescriptor": _d("BehaviorDescriptor", "School Code of Conduct"),
            "disciplineIncidentReference": {"incidentIdentifier": incident_id, "schoolId": sid},
            "studentReference": {"studentUniqueId": offender["studentUniqueId"]},
        })
        sdi_non_offender_assoc.append({
            "id": _uid("sdino", witness["studentUniqueId"], incident_id),
            "disciplineIncidentReference": {"incidentIdentifier": incident_id, "schoolId": sid},
            "studentReference": {"studentUniqueId": witness["studentUniqueId"]},
            "disciplineIncidentParticipationCodes": [
                {"disciplineIncidentParticipationCodeDescriptor": _d("DisciplineIncidentParticipationCodeDescriptor", "Witness")}],
        })
        discipline_actions.append({
            "id": _uid("da", offender["studentUniqueId"], f"DA-{sid}-001"),
            "disciplineActionIdentifier": f"DA-{sid}-001",
            "disciplineDate": f"{yr0}-10-16",
            "studentReference": {"studentUniqueId": offender["studentUniqueId"]},
            "assignmentSchoolReference": {"schoolId": sid},
            "responsibilitySchoolReference": {"schoolId": sid},
            "disciplines": [{"disciplineDescriptor": _d("DisciplineDescriptor", "In School Suspension")}],
        })
        restraint_events.append({
            "id": _uid("re", sid, "RE-001"),
            "restraintEventIdentifier": f"RE-{sid}-001",
            "schoolReference": {"schoolId": sid},
            "studentReference": {"studentUniqueId": offender["studentUniqueId"]},
            "eventDate": f"{yr0}-11-02",
            "educationalEnvironmentDescriptor": _d("EducationalEnvironmentDescriptor", "Classroom"),
        })
    out["discipline_incidents"] = discipline_incidents
    out["discipline_actions"] = discipline_actions
    out["student_discipline_incident_behavior_associations"] = sdi_behavior_assoc
    out["student_discipline_incident_non_offender_associations"] = sdi_non_offender_assoc
    out["restraint_events"] = restraint_events

    # ---- surveys ----
    svy = {"surveyIdentifier": "CLIMATE-1", "namespace": f"{_EDFI}/Survey"}
    out["surveys"] = [{
        "id": _uid("svy", svy["surveyIdentifier"]),
        **svy,
        "surveyTitle": "School Climate Survey",
        "schoolYearTypeReference": {"schoolYear": school_year},
    }]
    out["survey_sections"] = [{
        "id": _uid("svys", svy["surveyIdentifier"], t),
        "surveySectionTitle": t,
        "surveyReference": svy,
    } for t in ("Belonging", "Safety")]
    out["survey_questions"] = [{
        "id": _uid("svyq", svy["surveyIdentifier"], qc),
        "questionCode": qc,
        "surveyReference": svy,
        "questionText": qt,
        "questionFormDescriptor": _d("QuestionFormDescriptor", "Likert Scale"),
    } for qc, qt in [("Q1", "I feel like I belong at my school."), ("Q2", "I feel safe at my school.")]]
    survey_responses, survey_section_responses, survey_question_responses = [], [], []
    for stu in students[:30]:
        resp_id = f"R-{stu['studentUniqueId']}"
        resp_ref = {**svy, "surveyResponseIdentifier": resp_id}
        survey_responses.append({
            "id": _uid("svyr", resp_id),
            "surveyResponseIdentifier": resp_id,
            "surveyReference": svy,
            "studentReference": {"studentUniqueId": stu["studentUniqueId"]},
            "responseDate": f"{yr1}-03-01",
        })
        survey_section_responses.append({
            "id": _uid("svysr", resp_id, "Belonging"),
            "surveyResponseReference": resp_ref,
            "surveySectionReference": {**svy, "surveySectionTitle": "Belonging"},
            "sectionRating": rng.randint(1, 5),
        })
        survey_question_responses.append({
            "id": _uid("svyqr", resp_id, "Q1"),
            "surveyQuestionReference": {**svy, "questionCode": "Q1"},
            "surveyResponseReference": resp_ref,
            "values": [{"surveyQuestionResponseValueIdentifier": 1, "textValue": str(rng.randint(1, 5))}],
        })
    out["survey_responses"] = survey_responses
    out["survey_section_responses"] = survey_section_responses
    out["survey_question_responses"] = survey_question_responses
    out["survey_response_education_organization_target_associations"] = [{
        "id": _uid("svyt", r["surveyResponseIdentifier"]),
        "educationOrganizationReference": {"educationOrganizationId": 1000},
        "surveyResponseReference": {**svy, "surveyResponseIdentifier": r["surveyResponseIdentifier"]},
    } for r in survey_responses[:5]]

    # ---- TPDM ----
    candidate_records = [{
        "id": _uid("cand", f"CAND-{j:03d}"),
        "candidateIdentifier": f"CAND-{j:03d}",
        "firstName": rng.choice(first_names),
        "lastSurname": rng.choice(last_names),
        "birthDate": "1998-05-10",
        "sexDescriptor": _d("SexDescriptor", rng.choice(sex_choices)),
        "personReference": {"personId": f"P{j:04d}", "sourceSystemDescriptor": _d("SourceSystemDescriptor", "District")},
    } for j in range(8)]
    out["candidates"] = candidate_records
    epp = {"programName": "Demo Educator Prep",
           "programTypeDescriptor": _d("ProgramTypeDescriptor", "Educator preparation", base=_TPDM),
           "educationOrganizationId": schools[0]["schoolId"]}
    out["educator_preparation_programs"] = [{
        "id": _uid("epp", epp["programName"]),
        "programName": epp["programName"],
        "programTypeDescriptor": epp["programTypeDescriptor"],
        "educationOrganizationReference": {"educationOrganizationId": epp["educationOrganizationId"]},
    }]
    out["candidate_educator_preparation_program_associations"] = [{
        "id": _uid("ceppa", c["candidateIdentifier"]),
        "candidateReference": {"candidateIdentifier": c["candidateIdentifier"]},
        "educatorPreparationProgramReference": epp,
        "beginDate": f"{school_year - 2}-09-01",
        "endDate": f"{yr1}-06-15",
        "eppProgramPathwayDescriptor": _d("EPPProgramPathwayDescriptor", "Traditional", base=_TPDM),
    } for c in candidate_records]

    # ---- deletes: tombstones for records that ARE in the data above ----
    out["_deletes"] = {
        "student_section_associations": [
            {"id": r["id"], "changeVersion": 1} for r in student_section_assoc[:10]
        ],
        "student_school_attendance_events": [
            {"id": r["id"], "changeVersion": 1} for r in student_school_attend[:5]
        ],
        "staff_section_associations": [
            {"id": r["id"], "changeVersion": 1} for r in staff_section_assoc[:2]
        ],
    }
    # ---- descriptor resources: one record per (namespace, codeValue) the data
    #      uses, so Silver's `_descriptors` lookup resolves every code on every
    #      run. Honest in synthetic (codeValue is a real code the ODS defines);
    #      a real ODS supplies the same shape with real shortDescriptions.
    _emit_descriptors(out)
    return out


def _grade_band_for_school(idx: int) -> List[str]:
    bands = [
        ["Kindergarten", "First grade", "Second grade", "Third grade", "Fourth grade", "Fifth grade"],
        ["Sixth grade", "Seventh grade", "Eighth grade"],
        ["Ninth grade", "Tenth grade", "Eleventh grade", "Twelfth grade"],
    ]
    return bands[idx % len(bands)]
