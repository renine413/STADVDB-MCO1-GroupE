import React, { useState, useEffect } from "react";
import axios from "axios";
import {
  ScatterChart, Scatter, XAxis, YAxis, CartesianGrid, Tooltip,
  Radar, RadarChart, PolarGrid, PolarAngleAxis, PolarRadiusAxis,
  BarChart, Bar, Legend,
  LineChart, Line, ResponsiveContainer,
} from "recharts";
import "./App.css";

function Dashboard() {
  const [filters, setFilters] = useState({
    gender: "All",
    ageRange: "All",
  });

  const [sleepStress, setSleepStress] = useState([]);
  const [mentalRisk, setMentalRisk] = useState([]);
  const [socialImpact, setSocialImpact] = useState([]);
  const [genderAge, setGenderAge] = useState({ byGender: [], byAge: [] });
  const [trend, setTrend] = useState([]);

  const fetchData = async () => {
    const params = { gender: filters.gender, ageRange: filters.ageRange };
    const [s1, s2, s3, s4, s5] = await Promise.all([
      axios.get("/api/sleep-stress", { params }),
      axios.get("/api/mental-health-indicators", { params }),
      axios.get("/api/social-media-impact", { params }),
      axios.get("/api/gender-age-comparison", { params }),
      axios.get("/api/addiction-trend", { params }),
    ]);
    setSleepStress(s1.data);
    setMentalRisk(s2.data);
    setSocialImpact(s3.data);
    setGenderAge(s4.data);
    setTrend(s5.data);
  };

  useEffect(() => { fetchData(); }, [filters]);

  return (
    <div className="dashboard-container">
      <h1 className="dashboard-title">🧠 OLAP Mental Health Dashboard</h1>

      <div className="filter-card">
        <h2>OLAP Filters (Slice / Dice)</h2>
        <div className="filters">
          <label>
            Gender:
            <select
              name="gender"
              value={filters.gender}
              onChange={(e) => setFilters({ ...filters, gender: e.target.value })}
            >
              <option>All</option>
              <option>Male</option>
              <option>Female</option>
            </select>
          </label>

          <label>
            Age Range:
            <select
              name="ageRange"
              value={filters.ageRange}
              onChange={(e) => setFilters({ ...filters, ageRange: e.target.value })}
            >
              <option>All</option>
              <option>18–20</option>
              <option>21–23</option>
              <option>24+</option>
            </select>
          </label>
        </div>
      </div>

      {/* 1️⃣ Sleep vs Stress */}
{/* 1️⃣ Sleep vs Stress */}
  <div className="chart-card">
    <h3>1. Correlation between Sleep and Stress</h3>
    <ResponsiveContainer width="100%" height={300}>
      <ScatterChart>
        <CartesianGrid />
        <XAxis dataKey="sleep" name="Sleep (hrs)" />
        <YAxis dataKey="stress" name="Stress Level" domain={[0, 10]} />
        <Tooltip
          cursor={{ strokeDasharray: "3 3" }}
          formatter={(value, name, props) => [value, name === "sleep" ? "Sleep (hrs)" : "Stress"]}
          labelFormatter={() => ""}
          content={({ payload }) => {
            if (!payload || !payload.length) return null;
            const d = payload[0].payload;
            return (
              <div
                style={{
                  background: "white",
                  border: "1px solid #ccc",
                  padding: "6px 8px",
                  borderRadius: "8px"
                }}
              >
                <p><b>Gender:</b> {d.gender || "All"}</p>
                <p><b>Age Group:</b> {d.ageGroup || "All Ages"}</p>
                <p><b>Sleep (hrs):</b> {d.sleep}</p>
                <p><b>Stress:</b> {d.stress}</p>
              </div>
            );
          }}
        />
        <Scatter
          name="All Genders"
          data={sleepStress}
          fill="#8884d8"
          shape="circle"
          line={{ stroke: "#8884d8", strokeWidth: 1 }}
        />
      </ScatterChart>
    </ResponsiveContainer>
  </div>

      {/* 2️⃣ Mental Risk Indicators */}
      <div className="chart-card">
        <h3>2. Mental Risk Indicators</h3>
        <ResponsiveContainer width="100%" height={300}>
          <RadarChart data={mentalRisk}>
            <PolarGrid />
            <PolarAngleAxis dataKey="factor" />
            <PolarRadiusAxis domain={[0, 10]} />
            <Radar
              dataKey="value"
              stroke="#82ca9d"
              fill="#82ca9d"
              fillOpacity={0.6}
              name="Score"
            />
            <Tooltip
              formatter={(value, name, props) => [`${value}`, `${props.payload.factor}`]}
              contentStyle={{
                backgroundColor: "rgba(255,255,255,0.9)",
                borderRadius: "8px",
                border: "1px solid #ccc",
              }}
            />
          </RadarChart>
        </ResponsiveContainer>
      </div>


      {/* 3️⃣ Social Media Impact */}
      <div className="chart-card">
        <h3>3. Social Media Addiction vs Academic Performance</h3>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={socialImpact}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="level" />
            <YAxis domain={[0, 10]} />
            <Tooltip />
            <Legend />
            <Bar dataKey="gpa" fill="#8884d8" name="GPA" />
            <Bar dataKey="addiction" fill="#ff7f7f" name="Addiction" />
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* 4️⃣ Gender & Age Group */}
      <div className="chart-card">
        <h3>4. Gender & Age Group Comparison of Stress and Mental Health</h3>
        <p style={{ fontSize: "0.85rem", color: "#666" }}>
          (Note: Filters do not apply to this section)
        </p>
        <h4>By Gender</h4>
        <ResponsiveContainer width="100%" height={250}>
          <BarChart data={genderAge.byGender}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="gender" />
            <YAxis domain={[0, 10]} />
            <Tooltip />
            <Legend />
            <Bar dataKey="stress" fill="#ff7f7f" name="Stress" />
            <Bar dataKey="mentalHealth" fill="#82ca9d" name="Mental Health" />
          </BarChart>
        </ResponsiveContainer>

        <h4>By Age Group </h4>
        <ResponsiveContainer width="100%" height={250}>
          <LineChart data={genderAge.byAge}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="ageGroup" />
            <YAxis domain={[0, 10]} />
            <Tooltip />
            <Legend />
            <Line dataKey="stress" stroke="#ff7f7f" />
            <Line dataKey="mentalHealth" stroke="#82ca9d" />
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* 5️⃣ Trend */}
      <div className="chart-card">
        <h3>5. Social Media Addiction and Mental Health Trend</h3>
        <ResponsiveContainer width="100%" height={300}>
          <LineChart data={trend}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="ageGroup" />
            <YAxis domain={[0, 10]} />
            <Tooltip />
            <Legend />
            <Line dataKey="addiction" stroke="#8884d8" name="Addiction" />
            <Line dataKey="mentalHealth" stroke="#82ca9d" name="Mental Health" />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

export default Dashboard;
