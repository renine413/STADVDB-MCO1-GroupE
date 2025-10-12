-- 1. Correlation between Sleep and Stress (ROLL-UP)

SELECT 
    age_group,
    gender,
    ROUND(AVG(avg_sleep_hours_x), 2) AS avg_sleep,
    ROUND(AVG(avg_stress_level), 2) AS avg_stress,
    CASE 
        WHEN GROUPING(age_group) = 1 AND GROUPING(gender) = 1 THEN 'Overall Average'
        WHEN GROUPING(gender) = 1 THEN CONCAT(age_group, ' (All Genders)')
        ELSE CONCAT(age_group, ' - ', gender)
    END AS grouping_level
FROM fact_student_summary
GROUP BY ROLLUP (age_group, gender)
ORDER BY age_group, gender;

-- 2. Mental Risk Indicators (DICE)
SELECT 
    age_group,
    gender,
    ROUND(AVG(avg_stress_level), 2) AS avg_stress,
    ROUND(AVG(avg_addicted_score), 2) AS avg_addiction,
    ROUND(AVG(avg_mental_health_score), 2) AS avg_mh_score
FROM fact_student_summary
WHERE avg_stress_level > 1
  AND avg_addicted_score > 4
  AND avg_sleep_hours_x < 8
GROUP BY age_group, gender
ORDER BY avg_stress DESC;


-- 3. Impact of Social Media Addiction on Academic Performance
-- Step 1: Roll-up summary (all students)
SELECT 
    ROUND(AVG(avg_addicted_score), 2) AS overall_addiction,
    ROUND(AVG(avg_cgpa), 2) AS overall_cgpa,
    ROUND(AVG(avg_stress_level), 2) AS overall_stress
FROM fact_student_summary;

-- Step 2: Drill-down by gender and age group
SELECT 
    gender,
    age_group,
    ROUND(AVG(avg_addicted_score), 2) AS avg_addiction,
    ROUND(AVG(avg_cgpa), 2) AS avg_cgpa,
    ROUND(AVG(avg_stress_level), 2) AS avg_stress
FROM fact_student_summary
GROUP BY gender, age_group
ORDER BY gender, age_group;

-- 4. Gender & Age Group Comparison of Stress and Mental Health
-- Slice for Female students only
SELECT 
    age_group,
    ROUND(AVG(avg_stress_level), 2) AS avg_stress,
    ROUND(AVG(avg_mental_health_score), 2) AS avg_mh
FROM fact_student_summary
WHERE gender = 'Female'
GROUP BY age_group
ORDER BY age_group;


-- Slice for Male students only
SELECT 
    age_group,
    ROUND(AVG(avg_stress_level), 2) AS avg_stress,
    ROUND(AVG(avg_mental_health_score), 2) AS avg_mh
FROM fact_student_summary
WHERE gender = 'Male'
GROUP BY age_group
ORDER BY age_group;

-- Slice for Both genders 
SELECT 
    gender,
    age_group,
    ROUND(AVG(avg_stress_level), 2) AS avg_stress,
    ROUND(AVG(avg_mental_health_score), 2) AS avg_mh
FROM fact_student_summary
GROUP BY gender, age_group
ORDER BY age_group, gender;

-- 5. Social Media Addiction and Mental Health

SELECT 
    age_group,
    gender,
    ROUND(AVG(avg_addicted_score), 2) AS avg_addiction,
    ROUND(AVG(avg_mental_health_score), 2) AS avg_mental_health,
    ROUND(AVG(avg_academic_pressure), 2) AS avg_acad_pressure,
    ROUND(AVG(avg_work_study_hours), 2) AS avg_work_pressure,
    ROUND(AVG(avg_sleep_duration), 2) AS avg_sleep
FROM fact_student_summary
WHERE avg_addicted_score > 5
GROUP BY ROLLUP (age_group, gender)
ORDER BY age_group, gender;
