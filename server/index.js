import express from "express";
import cors from "cors";
import mysql from "mysql2";

const app = express();
app.use(cors());
app.use(express.json());

const db = mysql.createConnection({
  host: "localhost",
  user: "root",
  password: "password", // update if needed
  database: "olap_dashboard"
});

// Helper for filters
function getFilterClauses(gender, ageRange) {
  let genderClause = gender && gender !== "All" ? `gender = '${gender}'` : "1=1";
  let ageClause = "1=1";
  if (ageRange === "18–20") ageClause = "age BETWEEN 18 AND 20";
  else if (ageRange === "21–23") ageClause = "age BETWEEN 21 AND 23";
  else if (ageRange === "24+") ageClause = "age >= 24";
  return { genderClause, ageClause };
}

// Correlation between Sleep and Stress
app.get("/api/sleep-stress", (req, res) => {
  const { gender = "All", ageRange = "18–20" } = req.query;
  const { genderClause, ageClause } = getFilterClauses(gender, ageRange);

  const sql = `
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
    WHERE ${genderClause} AND ${ageClause }
    GROUP BY gender, age_group WITH ROLLUP;
  `;

  db.query(sql, (err, rows) => {
    if (err) return res.status(500).json({ error: err.message });
    const formatted = rows
      .filter(r => r.avg_sleep && r.avg_stress_level)
      .map(r => ({ sleep: r.avg_sleep, stress: r.avg_stress_level }));
    res.json(formatted);
  });
});

// Mental Risk Indicators
app.get("/api/mental-health-indicators", (req, res) => {
  const { gender = "All", ageRange = "18–20" } = req.query;
  const { genderClause, ageClause } = getFilterClauses(gender, ageRange);

  const sql = `
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
    WHERE ${genderClause} AND ${ageClause}
    GROUP BY gender, age_group WITH ROLLUP;
  `;

  db.query(sql, (err, rows) => {
    if (err) return res.status(500).json({ error: err.message });
    if (!rows.length) return res.json([]);

    const latest = rows[0]; // single group summary
    const formatted = [
      { factor: "Addiction", value: latest.avg_addicted_score || 0 },
      { factor: "Academic", value: latest.avg_cgpa || 0 },
      { factor: "Mental Health", value: latest.avg_mental_health_score || 0 },
    ];
    res.json(formatted);
  });
});

// Impact of Social Media Addiction on Academic Performance
app.get("/api/social-media-impact", (req, res) => {
  const { gender = "All", ageRange = "18–20" } = req.query;
  const { genderClause, ageClause } = getFilterClauses(gender, ageRange);

  const sql = `
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
    WHERE ${genderClause} AND ${ageClause}
    GROUP BY gender, age_group WITH ROLLUP
    ORDER BY gender, age_group;
  `;

  db.query(sql, (err, rows) => {
    if (err) return res.status(500).json({ error: err.message });
    const formatted = rows
      .filter(r => r.avg_addicted_score && r.avg_cgpa)
      .map(r => ({
        level: r.age_group || "All",
        addiction: r.avg_addicted_score,
        gpa: r.avg_cgpa
      }));
    res.json(formatted);
  });
});

// Gender & Age Group Comparison of Stress and Mental Health
app.get("/api/gender-age-comparison", (req, res) => {
  const { gender = "All", ageRange = "18–20" } = req.query;
  const { genderClause, ageClause } = getFilterClauses(gender, ageRange);

  const sqlGender = `
    SELECT 
        gender,
        ROUND(AVG(avg_stress_level), 2) AS avg_stress,
        ROUND(AVG(avg_mental_health_score), 2) AS avg_mh
    FROM fact_student_summary
    WHERE ${genderClause}
    GROUP BY gender WITH ROLLUP
    ORDER BY gender;
  `;

  const sqlAge = `
    SELECT 
        CASE 
            WHEN age BETWEEN 18 AND 20 THEN '18–20'
            WHEN age BETWEEN 21 AND 23 THEN '21–23'
            ELSE '24+'
        END AS age_group,
        ROUND(AVG(avg_stress_level), 2) AS avg_stress_level,
        ROUND(AVG(avg_mental_health_score), 2) AS avg_mental_health_score
    FROM fact_student_summary
    WHERE ${ageClause}
    GROUP BY age_group WITH ROLLUP
    ORDER BY 
        CASE 
            WHEN age_group = '18–20' THEN 1
            WHEN age_group = '21–23' THEN 2
            ELSE 3
        END;
  `;

  db.query(sqlGender, (err, genderRows) => {
    if (err) return res.status(500).json({ error: err.message });
    db.query(sqlAge, (err2, ageRows) => {
      if (err2) return res.status(500).json({ error: err2.message });
      res.json({
        byGender: genderRows.map(r => ({
          gender: r.gender || "All",
          stress: r.avg_stress || 0,
          mentalHealth: r.avg_mh || 0
        })),
        byAge: ageRows.map(r => ({
          ageGroup: r.age_group || "All",
          stress: r.avg_stress_level || 0,
          mentalHealth: r.avg_mental_health_score || 0
        }))
      });
    });
  });
});

// Social Media Addiction and Mental Health Trend
app.get("/api/addiction-trend", (req, res) => {
  const { gender = "All", ageRange = "18–20" } = req.query;
  const { genderClause, ageClause } = getFilterClauses(gender, ageRange);

  const sql = `
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
    WHERE ${genderClause} AND ${ageClause}
    GROUP BY age_group, gender WITH ROLLUP
    ORDER BY 
      CASE 
          WHEN age_group = '18–20' THEN 1
          WHEN age_group = '21–23' THEN 2
          ELSE 3
      END;
  `;

  db.query(sql, (err, rows) => {
    if (err) return res.status(500).json({ error: err.message });
    const formatted = rows
      .filter(r => r.avg_mental_health_score && r.avg_academic_pressure)
      .map(r => ({
        ageGroup: r.age_group || "All",
        addiction: r.avg_academic_pressure || 0,
        mentalHealth: r.avg_mental_health_score || 0
      }));
    res.json(formatted);
  });
});

app.listen(5000, () => console.log("✅ OLAP API running on http://localhost:5000"));
