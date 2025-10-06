-- Create staging database and tables for ETL (MySQL)
CREATE DATABASE IF NOT EXISTS dw_stage CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE dw_stage;

-- 1) CDC Indicators (from cdc-indicators-of-anxiety-or-depression.json)
CREATE TABLE IF NOT EXISTS stg_cdc_indicators (
  indicator VARCHAR(50) NOT NULL,
  `group` VARCHAR(50) NOT NULL,
  state VARCHAR(50) NOT NULL,
  subgroup VARCHAR(50) NOT NULL,
  phase DECIMAL(2,1) NULL,
  time_period INT NOT NULL,
  time_period_label VARCHAR(50) NULL,
  time_period_start_date DATE NULL,
  time_period_end_date DATE NULL,
  value DECIMAL(5,2) NULL,
  low_CI DECIMAL(5,2) NULL,
  high_CI DECIMAL(5,2) NULL,
  confidence_interval VARCHAR(50) NULL,
  quartile_range VARCHAR(50) NULL,
  PRIMARY KEY (indicator, `group`, state, subgroup, time_period, phase)
) ENGINE=InnoDB;

-- 2) Student Depression CSV (kaggle-student-depression-dataset.csv)
CREATE TABLE IF NOT EXISTS stg_student_depression_csv (
  id INT NOT NULL,
  gender VARCHAR(10) NULL,
  age INT NULL,
  city VARCHAR(50) NULL,
  profession VARCHAR(30) NULL,
  academic_pressure INT NULL,
  cgpa DECIMAL(3,2) NULL,
  study_satisfaction INT NULL,
  sleep_duration VARCHAR(30) NULL,
  dietary_habits VARCHAR(30) NULL,
  degree VARCHAR(20) NULL,
  has_suicidal_thoughts TINYINT(1) NULL,
  work_study_hours INT NULL,
  financial_stress INT NULL,
  has_mental_illness_family_history TINYINT(1) NULL,
  has_depression TINYINT(1) NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB;

-- 3) Student Mental Health Excel (kaggle-student-mental-health-crisis-after-covid19-final.xlsx)
CREATE TABLE IF NOT EXISTS stg_student_mental_health_excel (
  row_hash CHAR(32) NOT NULL,
  gender VARCHAR(10) NULL,
  age INT NULL,
  city VARCHAR(50) NULL,
  profession VARCHAR(30) NULL,
  academic_pressure INT NULL,
  cgpa DECIMAL(3,2) NULL,
  study_satisfaction INT NULL,
  sleep_duration VARCHAR(30) NULL,
  dietary_habits VARCHAR(30) NULL,
  degree VARCHAR(20) NULL,
  has_suicidal_thoughts TINYINT(1) NULL,
  work_study_hours INT NULL,
  financial_stress INT NULL,
  has_mental_illness_family_history TINYINT(1) NULL,
  has_depression TINYINT(1) NULL,
  PRIMARY KEY (row_hash)
) ENGINE=InnoDB;

-- 4) Student Performance JSON (kaggle-student-performance-and-behavior-dataset.json)
CREATE TABLE IF NOT EXISTS stg_student_performance_json (
  student_id VARCHAR(10) NOT NULL,
  gender VARCHAR(10) NULL,
  age INT NULL,
  department VARCHAR(20) NULL,
  attendance DECIMAL(5,2) NULL,
  midterm_score DECIMAL(5,2) NULL,
  final_score DECIMAL(5,2) NULL,
  assignments_ave DECIMAL(5,2) NULL,
  quizzes_ave DECIMAL(5,2) NULL,
  participation_score DECIMAL(5,2) NULL,
  projects_score DECIMAL(5,2) NULL,
  total_score DECIMAL(5,2) NULL,
  grade CHAR(2) NULL,
  study_hours_per_week DECIMAL(4,2) NULL,
  has_extracurricular_activities TINYINT(1) NULL,
  has_internet_access TINYINT(1) NULL,
  parent_education_level VARCHAR(20) NULL,
  family_income_level VARCHAR(10) NULL,
  stress_level INT NULL,
  sleep_hours DECIMAL(3,1) NULL,
  PRIMARY KEY (student_id)
) ENGINE=InnoDB;

-- 5) Social Media Addiction CSV (kaggle-students-social-media-addiction.csv)
CREATE TABLE IF NOT EXISTS stg_social_media_addiction (
  student_id INT NOT NULL,
  age INT NULL,
  gender VARCHAR(10) NULL,
  academic_level VARCHAR(30) NULL,
  country VARCHAR(30) NULL,
  ave_daily_usage_hours DECIMAL(3,1) NULL,
  most_used_platform VARCHAR(20) NULL,
  affects_academic_performance TINYINT(1) NULL,
  sleep_hours DECIMAL(3,1) NULL,
  mental_health_score INT NULL,
  relationship_status VARCHAR(30) NULL,
  conflicts_over_social_media INT NULL,
  addicted_score INT NULL,
  PRIMARY KEY (student_id)
) ENGINE=InnoDB;

-- 6) PHQ9 Mendeley CSV (mendeley-phq9-student-depression-dataset.csv)
CREATE TABLE IF NOT EXISTS stg_phq9_mendeley (
  row_hash CHAR(32) NOT NULL,
  age INT NULL,
  gender VARCHAR(10) NULL,
  has_interest_or_pleasure_in_doing_things VARCHAR(30) NULL,
  feel_down_depressed_hopeless VARCHAR(30) NULL,
  trouble_falling_staying_asleep_sleeping_too_much VARCHAR(30) NULL,
  feel_tired_having_little_energy VARCHAR(30) NULL,
  poor_appetite_overeating VARCHAR(30) NULL,
  fell_bad_failure_let_yourself_family_down VARCHAR(30) NULL,
  trouble_concentrating_reading_newspaper_watching_television VARCHAR(30) NULL,
  move_speak_slowly_noticed_fidgety_restless VARCHAR(30) NULL,
  better_off_dead_hurting_yourself VARCHAR(30) NULL,
  phq9_total_score INT NULL,
  depression_level VARCHAR(30) NULL,
  PRIMARY KEY (row_hash)
) ENGINE=InnoDB;

-- Simple ETL run log
CREATE TABLE IF NOT EXISTS etl_run_log (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  dataset VARCHAR(64) NOT NULL,
  started_at DATETIME NOT NULL,
  finished_at DATETIME NULL,
  rows_read INT NULL,
  rows_inserted INT NULL,
  rows_updated INT NULL,
  status VARCHAR(16) NOT NULL,
  message TEXT NULL,
  KEY idx_dataset_started_at (dataset, started_at)
) ENGINE=InnoDB;
