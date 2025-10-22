-- 1. Correlation between Sleep and Stress (ROLLUP, SLICE, DICE)
SELECT
    gender,
    CASE
        WHEN age BETWEEN 18 AND 20 THEN '18–20'
        WHEN age BETWEEN 21 AND 23 THEN '21–23'
        ELSE '24+'
    END AS age_group,
	ROUND(AVG(avg_sleep_duration), 2) AS avg_sleep,
    ROUND(AVG(avg_stress_level), 2) AS avg_stress_level
FROM fact_student_summary
WHERE gender ='Female' -- Application filter applied here
AND age BETWEEN 18 AND 20 -- Application filter applied here 
GROUP BY gender, age_group with ROLLUP;

-- 2. Mental Risk Indicators (ROLLUP, SLICE, DICE)
SELECT
    gender,
    CASE
        WHEN age BETWEEN 18 AND 20 THEN '18–20'
        WHEN age BETWEEN 21 AND 23 THEN '21–23'
        ELSE '24+'
    END AS age_group,
    ROUND(AVG(avg_addicted_score), 2) AS avg_addicted_score,
    ROUND(AVG(avg_cgpa), 2) AS avg_cgpa,
    ROUND(AVG(avg_mental_health_score), 2) AS avg_mental_health_score
FROM fact_student_summary
WHERE gender ='Female'  -- Application filter applied here
AND age BETWEEN 18 AND 20  -- Application filter applied here
GROUP BY gender, age_group with ROLLUP;

-- 3. Impact of Social Media Addiction on Academic Performance (ROLLUP, SLICE, DICE) 
SELECT 
    gender,
    CASE
        WHEN age BETWEEN 18 AND 20 THEN '18–20'
        WHEN age BETWEEN 21 AND 23 THEN '21–23'
        ELSE '24+'
    END AS age_group,
    ROUND(AVG(avg_addicted_score), 2) AS avg_addicted_score,
    ROUND(AVG(avg_cgpa), 2) AS avg_cgpa
FROM fact_student_summary
WHERE gender = 'Female'
  AND age BETWEEN 18 AND 20
GROUP BY gender, age_group WITH ROLLUP
ORDER BY gender, age_group;

-- 4. Gender & Age Group Comparison of Stress and Mental Health 
-- Age Group (ROLLUP, DICE)
SELECT 
    CASE 
        WHEN age BETWEEN 18 AND 20 THEN '18–20'
        WHEN age BETWEEN 21 AND 23 THEN '21–23'
        ELSE '24+'
    END AS age_group,
    ROUND(AVG(avg_stress_level), 2) AS avg_stress_level,
    ROUND(AVG(avg_mental_health_score), 2) AS avg_mental_health_score
FROM fact_student_summary
WHERE CASE
            WHEN age BETWEEN 18 AND 20 THEN '18–20'
            WHEN age BETWEEN 21 AND 23 THEN '21–23'
            ELSE '24+'
        END = '18–20'  -- Application filter applied here 
GROUP BY age_group WITH ROLLUP
ORDER BY 
    CASE 
        WHEN age_group = '18–20' THEN 1
        WHEN age_group = '21–23' THEN 2
        ELSE 3
    END;

-- Gender (DRILLDOWN)
SELECT 
    gender,
    ROUND(AVG(avg_stress_level), 2) AS avg_stress,
    ROUND(AVG(avg_mental_health_score), 2) AS avg_mh
FROM fact_student_summary
WHERE gender = 'Female' -- Application filter applied here 
GROUP BY gender WITH ROLLUP
ORDER BY gender;

-- 5. Social Media Addiction and Mental Health (ROLLUP, DICE)
SELECT
	gender, 
    CASE 
        WHEN age BETWEEN 18 AND 20 THEN '18–20'
        WHEN age BETWEEN 21 AND 23 THEN '21–23'
        ELSE '24+'
    END AS age_group,
    ROUND(AVG(avg_academic_pressure), 2) AS avg_academic_pressure,
    ROUND(AVG(avg_sleep_hours_x), 2) AS avg_sleep,
    ROUND(AVG(avg_work_study_hours), 2) AS avg_work_study_hours,
    ROUND(AVG(avg_mental_health_score), 2) AS avg_mental_health_score
FROM fact_student_summary
WHERE gender = 'Female' -- Application filter applied here
AND age BETWEEN 18 AND 20  -- Application filter applied here
GROUP BY age_group, gender WITH ROLLUP
ORDER BY 
    CASE 
        WHEN age_group = '18–20' THEN 1
        WHEN age_group = '21–23' THEN 2
        ELSE 3
    END;


