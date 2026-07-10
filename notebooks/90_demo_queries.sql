-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold-layer demo queries
-- MAGIC
-- MAGIC Example SQL that exercises the gold marts the way an
-- MAGIC analyst, dashboard developer, or Genie space would. Each section names
-- MAGIC the EDU concept it mirrors so you can compare to existing EDU
-- MAGIC dashboards if you have any.

-- COMMAND ----------

USE CATALOG edu_demo;
USE SCHEMA edu_gold;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Enrollment snapshot — students per school
-- MAGIC EDU dashboard equivalent: "School enrollment".

-- COMMAND ----------

SELECT
  s.school_name,
  s.lea_id,
  COUNT(DISTINCT f.k_student) AS enrolled_students,
  SUM(f.enrollment_days)      AS total_enrollment_days
FROM fct_student_school_association f
JOIN dim_school s USING (k_school)
GROUP BY s.school_name, s.lea_id
ORDER BY enrolled_students DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Average daily attendance by school
-- MAGIC EDU dashboard equivalent: "ADA / attendance rate".

-- COMMAND ----------

SELECT
  s.school_name,
  ROUND(AVG(m.attendance_rate) * 100, 2) AS avg_attendance_pct,
  SUM(m.total_days)   AS total_student_days,
  SUM(m.days_absent)  AS total_absence_days
FROM msr_student_cumulative_attendance m
JOIN dim_school s USING (k_school)
GROUP BY s.school_name
ORDER BY avg_attendance_pct DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Subgroup composition across the SEA
-- MAGIC EDU dashboard equivalent: "Subgroup demographics" — feeds federal reporting (Title I, IDEA, ELL).

-- COMMAND ----------

SELECT
  COUNT(*)                                                   AS total_students,
  SUM(CASE WHEN is_special_education THEN 1 ELSE 0 END)       AS special_ed,
  SUM(CASE WHEN is_english_learner   THEN 1 ELSE 0 END)       AS english_learners,
  SUM(CASE WHEN is_homeless          THEN 1 ELSE 0 END)       AS homeless,
  SUM(CASE WHEN is_title_i           THEN 1 ELSE 0 END)       AS title_i,
  SUM(CASE WHEN is_cte               THEN 1 ELSE 0 END)       AS cte,
  SUM(CASE WHEN is_food_service      THEN 1 ELSE 0 END)       AS food_service,
  SUM(CASE WHEN is_migrant           THEN 1 ELSE 0 END)       AS migrant
FROM fct_student_subgroup;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Course performance — average grade per course
-- MAGIC EDU dashboard equivalent: "Course outcomes".

-- COMMAND ----------

SELECT
  ds.local_course_title,
  ds.academic_subject := NULL,  -- subject is on dim_course, joined via course_code below
  ROUND(AVG(g.numeric_grade_earned), 2) AS avg_grade,
  COUNT(*)                              AS grades_recorded,
  ROUND(STDDEV(g.numeric_grade_earned), 2) AS grade_stddev
FROM fct_student_grades g
JOIN dim_course_section ds USING (k_course_section)
GROUP BY ds.local_course_title
ORDER BY avg_grade DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Discipline summary per school
-- MAGIC EDU dashboard equivalent: "Discipline incidents per school".

-- COMMAND ----------

SELECT
  s.school_name,
  COUNT(DISTINCT f.k_student)         AS students_with_actions,
  SUM(f.discipline_action_count)       AS total_actions
FROM fct_student_discipline_actions_summary f
JOIN dim_student stu USING (k_student)
JOIN fct_student_school_association sa USING (k_student)
JOIN dim_school s USING (k_school)
GROUP BY s.school_name
ORDER BY total_actions DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. Assessment performance
-- MAGIC EDU dashboard equivalent: "Assessment results".

-- COMMAND ----------

SELECT
  a.assessment_title,
  fsa.school_year,
  COUNT(*) AS students_assessed
FROM fct_student_assessment fsa
JOIN dim_assessment a USING (k_assessment)
GROUP BY a.assessment_title, fsa.school_year
ORDER BY students_assessed DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7. Staff-section assignments (who teaches what)
-- MAGIC EDU dashboard equivalent: "Teacher rosters".

-- COMMAND ----------

SELECT
  st.display_name AS teacher,
  cs.local_course_title,
  cs.section_identifier,
  fss.classroom_position
FROM fct_staff_section_association fss
JOIN dim_staff st USING (k_staff)
JOIN dim_course_section cs USING (k_course_section)
ORDER BY teacher, cs.local_course_title;
