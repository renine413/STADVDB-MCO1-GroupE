const express = require("express");
const cors = require("cors");
const mysql = require("mysql2");

const app = express();
app.use(cors());
app.use(express.json());

const db = mysql.createConnection({
  host: "localhost",
  user: "root",
  password: "password", // update as needed
  database: "olap_dashboard"
});

// 🔹 Helper for dynamic filter clauses
function getFilterClauses(gender, ageRange) {
  const genderClause = gender === "All" ? "1=1" : `gender = '${gender}'`;

  let ageClause = "1=1";
  if (ageRange === "18–20") ageClause = "age BETWEEN 18 AND 20";
  else if (ageRange === "21–23") ageClause = "age BETWEEN 21 AND 23";
  else if (ageRange === "24+") ageClause = "age >= 24";
  // "All" will leave ageClause = 1=1 (no restriction)
  return { genderClause, ageClause };
}

// ✅ 1️⃣ Correlation between Sleep and Stress
app.get("/api/sleep-stress", (req, res) => {
  const { gender = "All", ageRange = "All" } = req.query;
  const { genderClause, ageClause } = getFilterClauses(gender, ageRange);

  const ageCase = `
    CASE
      WHEN age BETWEEN 18 AND 20 THEN '18–20'
      WHEN age BETWEEN 21 AND 23 THEN '21–23'
      ELSE '24+'
    END AS age_group
  `;

  const sql = `
    SELECT
      gender,
      ${ageCase},
      ROUND(AVG(avg_sleep_duration), 2) AS avg_sleep,
      ROUND(AVG(avg_stress_level), 2) AS avg_stress_level
    FROM fact_student_summary
    WHERE ${genderClause} AND ${ageClause}
    GROUP BY gender, age_group;
  `;

  db.query(sql, (err, rows) => {
    if (err) {
      console.error("SQL ERROR in /api/sleep-stress:", err);
      return res.status(500).json({ error: err.message });
    }

    const formatted = rows
      .filter(r => r.avg_sleep !== null && r.avg_stress_level !== null)
      .map(r => ({
        gender: r.gender || "All",
        ageGroup: r.age_group || "All Ages",
        sleep: Number(r.avg_sleep),
        stress: Number(r.avg_stress_level)
      }));

    res.json(formatted);
  });
});




// ✅ 2️⃣ Mental Health Risk Indicators
app.get("/api/mental-health-indicators", (req, res) => {
  const { gender = "All", ageRange = "All" } = req.query;
  const { genderClause, ageClause } = getFilterClauses(gender, ageRange);

  const sql = `
    SELECT
        ROUND(AVG(avg_stress_level), 2) AS avg_stress,
        ROUND(AVG(avg_mental_health_score), 2) AS avg_mental_health,
        ROUND(AVG(avg_addicted_score), 2) AS avg_addiction,
        ROUND(AVG(avg_phq9_score), 2) AS avg_phq9
    FROM fact_student_summary
    WHERE ${genderClause} AND ${ageClause};
  `;

  db.query(sql, (err, rows) => {
    if (err) return res.status(500).json({ error: err.message });
    const r = rows[0] || {};

    const formatted = [
      { factor: "Stress", value: r.avg_stress || 0 },
      { factor: "Mental Health", value: r.avg_mental_health || 0 },
      { factor: "Addiction", value: r.avg_addiction || 0 },
      { factor: "PHQ-9 Score", value: r.avg_phq9 || 0 },
    ];
    res.json(formatted);
  });
});

// ✅ 3️⃣ Social Media Impact
app.get("/api/social-media-impact", (req, res) => {
  const { gender = "All", ageRange = "All" } = req.query;
  const { genderClause, ageClause } = getFilterClauses(gender, ageRange);

  const ageCase = `
    CASE
      WHEN age BETWEEN 18 AND 20 THEN '18–20'
      WHEN age BETWEEN 21 AND 23 THEN '21–23'
      ELSE '24+'
    END
  `;

  const groupBy =
    ageRange === "All"
      ? `GROUP BY ${ageCase} WITH ROLLUP`
      : ""; // no grouping when filtering to one range

  const sql = `
    SELECT 
      ${ageRange === "All" ? `${ageCase} AS age_group,` : ""}
      ROUND(AVG(avg_addicted_score), 2) AS avg_addicted_score,
      ROUND(AVG(avg_cgpa), 2) AS avg_cgpa
    FROM fact_student_summary
    WHERE ${genderClause} AND ${ageClause}
    ${groupBy};
  `;

  db.query(sql, (err, rows) => {
    if (err) return res.status(500).json({ error: err.message });

    let formatted;

    if (ageRange === "All") {
      formatted = rows
        .filter(r => r.avg_addicted_score && r.avg_cgpa)
        .map(r => ({
          level: r.age_group || "All Ages",
          addiction: r.avg_addicted_score,
          gpa: r.avg_cgpa,
        }));
    } else {
      const r = rows[0] || {};
      formatted = [
        {
          level: ageRange,
          addiction: r.avg_addicted_score || 0,
          gpa: r.avg_cgpa || 0,
        },
      ];
    }

    res.json(formatted);
  });
});


// ✅ 4️⃣ Gender & Age Group Comparison
app.get("/api/gender-age-comparison", (req, res) => {
  const sqlGender = `
    SELECT 
        gender,
        ROUND(AVG(avg_stress_level), 2) AS avg_stress,
        ROUND(AVG(avg_mental_health_score), 2) AS avg_mh
    FROM fact_student_summary
    GROUP BY gender WITH ROLLUP;
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
    GROUP BY age_group WITH ROLLUP;
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
          ageGroup: r.age_group || "All Ages",
          stress: r.avg_stress_level || 0,
          mentalHealth: r.avg_mental_health_score || 0
        }))
      });
    });
  });
});


// ✅ 5️⃣ Social Media Addiction & Mental Health Trend
app.get("/api/addiction-trend", (req, res) => {
  const { gender = "All", ageRange = "All" } = req.query;
  const { genderClause, ageClause } = getFilterClauses(gender, ageRange);

  const ageCase = `
    CASE 
        WHEN age BETWEEN 18 AND 20 THEN '18–20'
        WHEN age BETWEEN 21 AND 23 THEN '21–23'
        ELSE '24+'
    END
  `;

  // only group if we are viewing "All" age ranges
  const groupBy = ageRange === "All" ? `GROUP BY ${ageCase} WITH ROLLUP` : "";

  const sql = `
    SELECT
      ${ageRange === "All" ? `${ageCase} AS age_group,` : ""}
      ROUND(AVG(avg_academic_pressure), 2) AS avg_academic_pressure,
      ROUND(AVG(avg_mental_health_score), 2) AS avg_mental_health_score
    FROM fact_student_summary
    WHERE ${genderClause} AND ${ageClause}
    ${groupBy};
  `;

  db.query(sql, (err, rows) => {
    if (err) return res.status(500).json({ error: err.message });

    let formatted;
    if (ageRange === "All") {
      formatted = rows
        .filter(r => r.avg_academic_pressure && r.avg_mental_health_score)
        .map(r => ({
          ageGroup: r.age_group || "All Ages",
          addiction: r.avg_academic_pressure,
          mentalHealth: r.avg_mental_health_score,
        }));
    } else {
      const r = rows[0] || {};
      formatted = [
        {
          ageGroup: ageRange,
          addiction: r.avg_academic_pressure || 0,
          mentalHealth: r.avg_mental_health_score || 0,
        },
      ];
    }

    res.json(formatted);
  });
});


app.listen(5000, () =>
  console.log(" OLAP API running on http://localhost:5000")
);
