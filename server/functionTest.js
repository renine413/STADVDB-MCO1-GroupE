const axios = require("axios");

const baseURL = "http://localhost:5000";

const filters = [
  { gender: "All", ageRange: "All" },
  { gender: "Female", ageRange: "18–20" },
  { gender: "Male", ageRange: "21–23" },
  { gender: "Female", ageRange: "24+" },
];

const endpoints = [
  "sleep-stress",
  "mental-health-indicators",
  "social-media-impact",
  "gender-age-comparison",
  "addiction-trend",
];

async function testEndpoints() {
  for (const f of filters) {
    console.log(`\n=== Testing with Gender=${f.gender}, AgeRange=${f.ageRange} ===`);
    for (const ep of endpoints) {
      try {
        const res = await axios.get(`${baseURL}/api/${ep}`, { params: f });
        console.log(`${ep} ✅ PASS — returned ${res.data.length || 1} rows`);
      } catch (err) {
        console.error(`❌ ${ep} FAILED:`, err.response?.data || err.message);
      }
    }
  }
}

testEndpoints();
