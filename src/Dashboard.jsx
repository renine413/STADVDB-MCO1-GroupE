import React, { useState } from "react";
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
    ageRange: "18–25",
    year: "2024",
  });

  const handleFilterChange = (e) => {
    setFilters({ ...filters, [e.target.name]: e.target.value });
  };

  // Dummy data for all charts
  const sleepStressData = [
    { sleep: 4, stress: 8 },
    { sleep: 5, stress: 6 },
    { sleep: 6, stress: 5 },
    { sleep: 7, stress: 3 },
    { sleep: 8, stress: 2 },
  ];

  const radarData = [
    { factor: "Stress", value: 7 },
    { factor: "Anxiety", value: 6 },
    { factor: "Depression", value: 5 },
    { factor: "Sleep Quality", value: 8 },
    { factor: "Happiness", value: 6 },
  ];

  const socialMediaData = [
    { level: "Low", gpa: 3.5 },
    { level: "Moderate", gpa: 2.8 },
    { level: "High", gpa: 2.2 },
  ];

  const genderAgeData = [
    { group: "Male 18–25", stress: 7, health: 5 },
    { group: "Female 18–25", stress: 8, health: 4 },
    { group: "Male 26–35", stress: 6, health: 6 },
    { group: "Female 26–35", stress: 7, health: 5 },
  ];

  const addictionTrendData = [
    { year: 2020, addiction: 3, mentalHealth: 7 },
    { year: 2021, addiction: 4, mentalHealth: 6 },
    { year: 2022, addiction: 5, mentalHealth: 5 },
    { year: 2023, addiction: 6, mentalHealth: 4 },
    { year: 2024, addiction: 7, mentalHealth: 3 },
  ];

  return (
    <div className="dashboard-container">
      <h1 className="dashboard-title">🧠 Mental Health OLAP Dashboard</h1>

      {/* Filters */}
      <div className="filter-card">
        <h2>Query Parameters</h2>
        <div className="filters">
          <label>
            Gender:
            <select name="gender" value={filters.gender} onChange={handleFilterChange}>
              <option>All</option>
              <option>Male</option>
              <option>Female</option>
            </select>
          </label>
          <label>
            Age Range:
            <select name="ageRange" value={filters.ageRange} onChange={handleFilterChange}>
              <option>18–25</option>
              <option>26–35</option>
            </select>
          </label>
          <label>
            Year:
            <select name="year" value={filters.year} onChange={handleFilterChange}>
              <option>2023</option>
              <option>2024</option>
            </select>
          </label>
          <button>Apply Filters</button>
        </div>
      </div>

      {/* Charts Section */}
      <div className="charts-container">

        {/* 1️⃣ Scatter Plot */}
        <div className="chart-card">
          <h3>1. Correlation between Sleep and Stress</h3>
          <ResponsiveContainer width="100%" height={300}>
            <ScatterChart>
              <CartesianGrid />
              <XAxis type="number" dataKey="sleep" name="Sleep (hours)" />
              <YAxis type="number" dataKey="stress" name="Stress Level" />
              <Tooltip cursor={{ strokeDasharray: "3 3" }} />
              <Scatter data={sleepStressData} fill="#8884d8" />
            </ScatterChart>
          </ResponsiveContainer>
        </div>

        {/* 2️⃣ Radar Chart */}
        <div className="chart-card">
          <h3>2. Mental Health Risk Indicators</h3>
          <ResponsiveContainer width="100%" height={300}>
            <RadarChart data={radarData}>
              <PolarGrid />
              <PolarAngleAxis dataKey="factor" />
              <PolarRadiusAxis />
              <Radar dataKey="value" stroke="#82ca9d" fill="#82ca9d" fillOpacity={0.6} />
            </RadarChart>
          </ResponsiveContainer>
        </div>

        {/* 3️⃣ Bar Chart */}
        <div className="chart-card">
          <h3>3. Impact of Social Media Addiction on Academic Performance</h3>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={socialMediaData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="level" />
              <YAxis />
              <Tooltip />
              <Legend />
              <Bar dataKey="gpa" fill="#8884d8" />
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* 4️⃣ Gender & Age Group Comparison of Stress and Mental Health */}
        <div className="chart-card">
          <h3>4. Gender & Age Group Comparison of Stress and Mental Health</h3>

          {/* Bar Chart — Comparison by Gender */}
          <h4 style={{ textAlign: "center", marginTop: "0.5rem" }}>By Gender</h4>
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={[
              { gender: "Male", stress: 7, mentalHealth: 6 },
              { gender: "Female", stress: 8, mentalHealth: 5 },
            ]}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="gender" />
              <YAxis />
              <Tooltip />
              <Legend />
              <Bar dataKey="stress" fill="#ff7f7f" name="Stress Level" />
              <Bar dataKey="mentalHealth" fill="#82ca9d" name="Mental Health" />
            </BarChart>
          </ResponsiveContainer>

          {/* Line Chart — Comparison by Age Group */}
          <h4 style={{ textAlign: "center", marginTop: "1rem" }}>By Age Group</h4>
          <ResponsiveContainer width="100%" height={250}>
            <LineChart data={[
              { ageGroup: "18–25", stress: 8, mentalHealth: 5 },
              { ageGroup: "26–35", stress: 6, mentalHealth: 6 },
              { ageGroup: "36–45", stress: 5, mentalHealth: 7 },
            ]}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="ageGroup" />
              <YAxis />
              <Tooltip />
              <Legend />
              <Line type="monotone" dataKey="stress" stroke="#ff7f7f" name="Stress Level" />
              <Line type="monotone" dataKey="mentalHealth" stroke="#82ca9d" name="Mental Health" />
            </LineChart>
          </ResponsiveContainer>
        </div>

        {/* 5️⃣ Line Chart */}
        <div className="chart-card">
          <h3>5. Social Media Addiction and Mental Health (Trend)</h3>
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={addictionTrendData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="year" />
              <YAxis />
              <Tooltip />
              <Legend />
              <Line type="monotone" dataKey="addiction" stroke="#8884d8" />
              <Line type="monotone" dataKey="mentalHealth" stroke="#82ca9d" />
            </LineChart>
          </ResponsiveContainer>
        </div>

      </div>
    </div>
  );
}

export default Dashboard;
