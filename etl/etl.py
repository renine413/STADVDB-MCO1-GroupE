import os
import sys
import json
import hashlib
import math
import time
from datetime import datetime
from typing import Optional, List, Dict, Any, Iterable

import mysql.connector as mysql
from mysql.connector.cursor import MySQLCursor
import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm

# ------------------
# Config & Utilities
# ------------------

def load_config():
    load_dotenv()
    cfg = {
        'host': os.getenv('MYSQL_HOST', 'localhost'),
        'port': int(os.getenv('MYSQL_PORT', '3306')),
        'user': os.getenv('MYSQL_USER', 'root'),
        'password': os.getenv('MYSQL_PASSWORD', ''),
        'db_stage': os.getenv('MYSQL_DB_STAGE', 'dw_stage'),
        'batch_size': int(os.getenv('ETL_BATCH_SIZE', '2000')),
    }
    return cfg


def connect_mysql(db: Optional[str] = None):
    cfg = load_config()
    params = {
        'host': cfg['host'],
        'port': cfg['port'],
        'user': cfg['user'],
        'password': cfg['password'],
    }
    if db:
        params['database'] = db
    return mysql.connect(**params)


def execute_script(cursor: MySQLCursor, script_path: str):
    with open(script_path, 'r', encoding='utf-8') as f:
        sql_text = f.read()
    for statement in filter(None, [s.strip() for s in sql_text.split(';')]):
        if statement:
            cursor.execute(statement)


def upsert_log_start(cursor: MySQLCursor, dataset: str) -> int:
    cursor.execute(
        """
        INSERT INTO etl_run_log (dataset, started_at, status)
        VALUES (%s, %s, %s)
        """,
        (dataset, datetime.now(), 'running')
    )
    return cursor.lastrowid


def upsert_log_finish(cursor: MySQLCursor, log_id: int, rows_read: int, rows_inserted: int, rows_updated: int, status: str, message: Optional[str] = None):
    cursor.execute(
        """
        UPDATE etl_run_log
           SET finished_at = %s,
               rows_read = %s,
               rows_inserted = %s,
               rows_updated = %s,
               status = %s,
               message = %s
         WHERE id = %s
        """,
        (datetime.now(), rows_read, rows_inserted, rows_updated, status, message, log_id)
    )


# --------------
# Data Converters
# --------------

def to_bool(val: Any) -> Optional[int]:
    if val is None:
        return None
    s = str(val).strip().lower()
    if s in {'1', 'true', 't', 'yes', 'y'}:
        return 1
    if s in {'0', 'false', 'f', 'no', 'n'}:
        return 0
    return None


def to_int(val: Any) -> Optional[int]:
    try:
        if val is None or (isinstance(val, float) and math.isnan(val)):
            return None
        s = str(val).strip()
        if s == '':
            return None
        return int(float(s))
    except Exception:
        return None


def to_float(val: Any) -> Optional[float]:
    try:
        if val is None or (isinstance(val, float) and math.isnan(val)):
            return None
        s = str(val).replace(',', '').strip()
        if s == '':
            return None
        return float(s)
    except Exception:
        return None


def to_date(val: Any) -> Optional[str]:
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    # Try multiple formats
    fmts = ['%m/%d/%Y', '%Y-%m-%d', '%d/%m/%Y']
    for fmt in fmts:
        try:
            return datetime.strptime(s, fmt).strftime('%Y-%m-%d')
        except Exception:
            continue
    return None


def norm_gender(val: Any) -> Optional[str]:
    if val is None:
        return None
    s = str(val).strip().lower()
    if s.startswith('m'):
        return 'Male'
    if s.startswith('f'):
        return 'Female'
    return s.title() if s else None


def md5_hash_fields(fields: List[Any]) -> str:
    joined = '||'.join('' if v is None else str(v) for v in fields)
    return hashlib.md5(joined.encode('utf-8')).hexdigest()


# -----------------
# Loaders per dataset
# -----------------

def load_cdc_indicators(conn):
    cfg = load_config()
    cursor = conn.cursor()
    dataset = 'cdc_indicators'
    log_id = upsert_log_start(cursor, dataset)
    rows_read = rows_inserted = rows_updated = 0

    try:
        path = os.path.join(os.path.dirname(__file__), '..', 'datasets', 'cdc-indicators-of-anxiety-or-depression.json')
        path = os.path.abspath(path)
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        rows_read = len(data)

        insert_sql = (
            """
            INSERT INTO stg_cdc_indicators (
                indicator, `group`, state, subgroup, phase, time_period,
                time_period_label, time_period_start_date, time_period_end_date,
                value, low_CI, high_CI, confidence_interval, quartile_range
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                time_period_label = VALUES(time_period_label),
                time_period_start_date = VALUES(time_period_start_date),
                time_period_end_date = VALUES(time_period_end_date),
                value = VALUES(value),
                low_CI = VALUES(low_CI),
                high_CI = VALUES(high_CI),
                confidence_interval = VALUES(confidence_interval),
                quartile_range = VALUES(quartile_range)
            """
        )

        batch = []
        for rec in tqdm(data, desc='CDC', unit='row'):
            indicator = (rec.get('Indicator') or '')[:50]
            group = (rec.get('Group') or '')[:50]
            state = (rec.get('State') or '')[:50]
            subgroup = (rec.get('Subgroup') or '')[:50]
            phase = to_float(rec.get('Phase'))
            time_period = to_int(rec.get('Time Period'))
            tpl = (rec.get('Time Period Label') or '')[:50]
            tps = to_date(rec.get('Time Period Start Date'))
            tpe = to_date(rec.get('Time Period End Date'))
            value = to_float(rec.get('Value'))
            low = to_float(rec.get('Low CI'))
            high = to_float(rec.get('High CI'))
            ci = (rec.get('Confidence Interval') or '')[:50]
            qr = (rec.get('Quartile Range') or '')[:50]

            batch.append((indicator, group, state, subgroup, phase, time_period,
                          tpl, tps, tpe, value, low, high, ci, qr))

            if len(batch) >= cfg['batch_size']:
                cursor.executemany(insert_sql, batch)
                rows_inserted += cursor.rowcount  # includes updated rows; adjust later if needed
                batch.clear()
        if batch:
            cursor.executemany(insert_sql, batch)
            rows_inserted += cursor.rowcount

        conn.commit()
        upsert_log_finish(cursor, log_id, rows_read, rows_inserted, rows_updated, 'success', None)
        conn.commit()
        print(f"CDC load complete: read={rows_read}, affected={rows_inserted}")

    except Exception as e:
        conn.rollback()
        upsert_log_finish(cursor, log_id, rows_read, rows_inserted, rows_updated, 'failed', str(e))
        conn.commit()
        print(f"CDC load failed: {e}")
        raise


def load_student_depression_csv(conn):
    cfg = load_config()
    cursor = conn.cursor()
    dataset = 'student_depression_csv'
    log_id = upsert_log_start(cursor, dataset)
    rows_read = rows_inserted = rows_updated = 0

    try:
        path = os.path.join(os.path.dirname(__file__), '..', 'datasets', 'kaggle-student-depression-dataset.csv')
        path = os.path.abspath(path)
        df = pd.read_csv(path)
        rows_read = len(df)

        # Normalize columns
        def b(x): return to_bool(x)
        df2 = pd.DataFrame({
            'id': df['id'],
            'gender': df['Gender'].apply(norm_gender),
            'age': df['Age'].apply(to_int),
            'city': df['City'].astype(str).str.strip().str[:50],
            'profession': df['Profession'].astype(str).str.strip().str[:30],
            'academic_pressure': df['Academic Pressure'].apply(to_int),
            'cgpa': df['CGPA'].apply(to_float),
            'study_satisfaction': df['Study Satisfaction'].apply(to_int),
            'sleep_duration': df['Sleep Duration'].astype(str).str.strip().str[:30],
            'dietary_habits': df['Dietary Habits'].astype(str).str.strip().str[:30],
            'degree': df['Degree'].astype(str).str.strip().str[:20],
            'has_suicidal_thoughts': df['Have you ever had suicidal thoughts ?'].apply(b),
            'work_study_hours': df['Work/Study Hours'].apply(to_int),
            'financial_stress': df['Financial Stress'].apply(to_int),
            'has_mental_illness_family_history': df['Family History of Mental Illness'].apply(b),
            'has_depression': df['Depression'].apply(to_bool),
        })

        insert_sql = (
            """
            INSERT INTO stg_student_depression_csv (
              id, gender, age, city, profession, academic_pressure, cgpa, study_satisfaction,
              sleep_duration, dietary_habits, degree, has_suicidal_thoughts,
              work_study_hours, financial_stress, has_mental_illness_family_history,
              has_depression
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
              gender=VALUES(gender), age=VALUES(age), city=VALUES(city), profession=VALUES(profession),
              academic_pressure=VALUES(academic_pressure), cgpa=VALUES(cgpa), study_satisfaction=VALUES(study_satisfaction),
              sleep_duration=VALUES(sleep_duration), dietary_habits=VALUES(dietary_habits), degree=VALUES(degree),
              has_suicidal_thoughts=VALUES(has_suicidal_thoughts), work_study_hours=VALUES(work_study_hours),
              financial_stress=VALUES(financial_stress), has_mental_illness_family_history=VALUES(has_mental_illness_family_history),
              has_depression=VALUES(has_depression)
            """
        )

        batch = []
        for row in tqdm(df2.itertuples(index=False), total=len(df2), desc='StudentDepCSV'):
            batch.append(tuple(row))
            if len(batch) >= cfg['batch_size']:
                cursor.executemany(insert_sql, batch)
                rows_inserted += cursor.rowcount
                batch.clear()
        if batch:
            cursor.executemany(insert_sql, batch)
            rows_inserted += cursor.rowcount
        conn.commit()
        upsert_log_finish(cursor, log_id, rows_read, rows_inserted, rows_updated, 'success', None)
        conn.commit()
        print(f"Student Depression CSV load complete: read={rows_read}, affected={rows_inserted}")

    except Exception as e:
        conn.rollback()
        upsert_log_finish(cursor, log_id, rows_read, rows_inserted, rows_updated, 'failed', str(e))
        conn.commit()
        print(f"Student Depression CSV load failed: {e}")
        raise


def load_student_mental_health_excel(conn):
    cfg = load_config()
    cursor = conn.cursor()
    dataset = 'student_mental_health_excel'
    log_id = upsert_log_start(cursor, dataset)
    rows_read = rows_inserted = rows_updated = 0

    try:
        path = os.path.join(os.path.dirname(__file__), '..', 'datasets', 'kaggle-student-mental-health-crisis-after-covid19-final.xlsx')
        path = os.path.abspath(path)
        df = pd.read_excel(path)
        rows_read = len(df)

        def b(x): return to_bool(x)
        df2 = pd.DataFrame({
            'gender': df['Gender'].apply(norm_gender),
            'age': df['Age'].apply(to_int),
            'city': df['City'].astype(str).str.strip().str[:50],
            'profession': df['Profession'].astype(str).str.strip().str[:30],
            'academic_pressure': df['Academic Pressure'].apply(to_int),
            'cgpa': df['CGPA'].apply(to_float),
            'study_satisfaction': df['Study Satisfaction'].apply(to_int),
            'sleep_duration': df['Sleep Duration'].astype(str).str.strip().str[:30],
            'dietary_habits': df['Dietary Habits'].astype(str).str.strip().str[:30],
            'degree': df['Degree'].astype(str).str.strip().str[:20],
            'has_suicidal_thoughts': df['Have you ever had suicidal thoughts ?'].apply(b),
            'work_study_hours': df['Work/Study Hours'].apply(to_int),
            'financial_stress': df['Financial Stress'].apply(to_int),
            'has_mental_illness_family_history': df['Family History of Mental Illness'].apply(b),
            'has_depression': df['Depression'].apply(to_bool),
        })
        # row hash for idempotency
        df2['row_hash'] = df2.apply(lambda r: md5_hash_fields(list(r.values)), axis=1)
        # Reorder to put row_hash first
        cols = ['row_hash'] + [c for c in df2.columns if c != 'row_hash']
        df2 = df2[cols]

        insert_sql = (
            """
            INSERT INTO stg_student_mental_health_excel (
              row_hash, gender, age, city, profession, academic_pressure, cgpa, study_satisfaction,
              sleep_duration, dietary_habits, degree, has_suicidal_thoughts,
              work_study_hours, financial_stress, has_mental_illness_family_history,
              has_depression
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
              gender=VALUES(gender), age=VALUES(age), city=VALUES(city), profession=VALUES(profession),
              academic_pressure=VALUES(academic_pressure), cgpa=VALUES(cgpa), study_satisfaction=VALUES(study_satisfaction),
              sleep_duration=VALUES(sleep_duration), dietary_habits=VALUES(dietary_habits), degree=VALUES(degree),
              has_suicidal_thoughts=VALUES(has_suicidal_thoughts), work_study_hours=VALUES(work_study_hours),
              financial_stress=VALUES(financial_stress), has_mental_illness_family_history=VALUES(has_mental_illness_family_history),
              has_depression=VALUES(has_depression)
            """
        )

        batch = []
        for row in tqdm(df2.itertuples(index=False), total=len(df2), desc='StudentMHExcel'):
            batch.append(tuple(row))
            if len(batch) >= cfg['batch_size']:
                cursor.executemany(insert_sql, batch)
                rows_inserted += cursor.rowcount
                batch.clear()
        if batch:
            cursor.executemany(insert_sql, batch)
            rows_inserted += cursor.rowcount
        conn.commit()
        upsert_log_finish(cursor, log_id, rows_read, rows_inserted, rows_updated, 'success', None)
        conn.commit()
        print(f"Student Mental Health Excel load complete: read={rows_read}, affected={rows_inserted}")

    except Exception as e:
        conn.rollback()
        upsert_log_finish(cursor, log_id, rows_read, rows_inserted, rows_updated, 'failed', str(e))
        conn.commit()
        print(f"Student Mental Health Excel load failed: {e}")
        raise


def load_student_performance_json(conn):
    cfg = load_config()
    cursor = conn.cursor()
    dataset = 'student_performance_json'
    log_id = upsert_log_start(cursor, dataset)
    rows_read = rows_inserted = rows_updated = 0

    try:
        path = os.path.join(os.path.dirname(__file__), '..', 'datasets', 'kaggle-student-performance-and-behavior-dataset.json')
        path = os.path.abspath(path)
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        rows_read = len(data)

        insert_sql = (
            """
            INSERT INTO stg_student_performance_json (
              student_id, gender, age, department, attendance, midterm_score, final_score,
              assignments_ave, quizzes_ave, participation_score, projects_score, total_score,
              grade, study_hours_per_week, has_extracurricular_activities, has_internet_access,
              parent_education_level, family_income_level, stress_level, sleep_hours
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
              gender=VALUES(gender), age=VALUES(age), department=VALUES(department), attendance=VALUES(attendance),
              midterm_score=VALUES(midterm_score), final_score=VALUES(final_score), assignments_ave=VALUES(assignments_ave),
              quizzes_ave=VALUES(quizzes_ave), participation_score=VALUES(participation_score), projects_score=VALUES(projects_score),
              total_score=VALUES(total_score), grade=VALUES(grade), study_hours_per_week=VALUES(study_hours_per_week),
              has_extracurricular_activities=VALUES(has_extracurricular_activities), has_internet_access=VALUES(has_internet_access),
              parent_education_level=VALUES(parent_education_level), family_income_level=VALUES(family_income_level),
              stress_level=VALUES(stress_level), sleep_hours=VALUES(sleep_hours)
            """
        )

        batch = []
        for rec in tqdm(data, desc='StudentPerfJSON', unit='row'):
            # skip removed fields
            student_id = (rec.get('Student_ID') or '')[:10]
            gender = norm_gender(rec.get('Gender'))
            age = to_int(rec.get('Age'))
            department = (rec.get('Department') or '')[:20]
            attendance = to_float(rec.get('Attendance (%)'))
            midterm_score = to_float(rec.get('Midterm_Score'))
            final_score = to_float(rec.get('Final_Score'))
            assignments_ave = to_float(rec.get('Assignments_Avg'))
            quizzes_ave = to_float(rec.get('Quizzes_Avg'))
            participation_score = to_float(rec.get('Participation_Score'))
            projects_score = to_float(rec.get('Projects_Score'))
            total_score = to_float(rec.get('Total_Score'))
            grade = (rec.get('Grade') or '')[:2]
            study_hours_per_week = to_float(rec.get('Study_Hours_per_Week'))
            has_extracurricular_activities = to_bool(rec.get('Extracurricular_Activities'))
            has_internet_access = to_bool(rec.get('Internet_Access_at_Home'))
            parent_education_level = (rec.get('Parent_Education_Level') or '')[:20]
            family_income_level = (rec.get('Family_Income_Level') or '')[:10]
            stress_level = to_int(rec.get('Stress_Level (1-10)'))
            sleep_hours = to_float(rec.get('Sleep_Hours_per_Night'))

            batch.append((student_id, gender, age, department, attendance, midterm_score, final_score,
                          assignments_ave, quizzes_ave, participation_score, projects_score, total_score, grade,
                          study_hours_per_week, has_extracurricular_activities, has_internet_access,
                          parent_education_level, family_income_level, stress_level, sleep_hours))

            if len(batch) >= cfg['batch_size']:
                cursor.executemany(insert_sql, batch)
                rows_inserted += cursor.rowcount
                batch.clear()
        if batch:
            cursor.executemany(insert_sql, batch)
            rows_inserted += cursor.rowcount
        conn.commit()
        upsert_log_finish(cursor, log_id, rows_read, rows_inserted, rows_updated, 'success', None)
        conn.commit()
        print(f"Student Performance JSON load complete: read={rows_read}, affected={rows_inserted}")

    except Exception as e:
        conn.rollback()
        upsert_log_finish(cursor, log_id, rows_read, rows_inserted, rows_updated, 'failed', str(e))
        conn.commit()
        print(f"Student Performance JSON load failed: {e}")
        raise


def load_social_media_addiction(conn):
    cfg = load_config()
    cursor = conn.cursor()
    dataset = 'social_media_addiction'
    log_id = upsert_log_start(cursor, dataset)
    rows_read = rows_inserted = rows_updated = 0

    try:
        path = os.path.join(os.path.dirname(__file__), '..', 'datasets', 'kaggle-students-social-media-addiction.csv')
        path = os.path.abspath(path)
        df = pd.read_csv(path)
        rows_read = len(df)

        def b(x): return to_bool(x)
        df2 = pd.DataFrame({
            'student_id': df['Student_ID'].apply(to_int),
            'age': df['Age'].apply(to_int),
            'gender': df['Gender'].apply(norm_gender),
            'academic_level': df['Academic_Level'].astype(str).str.strip().str[:30],
            'country': df['Country'].astype(str).str.strip().str[:30],
            'ave_daily_usage_hours': df['Avg_Daily_Usage_Hours'].apply(to_float),
            'most_used_platform': df['Most_Used_Platform'].astype(str).str.strip().str[:20],
            'affects_academic_performance': df['Affects_Academic_Performance'].apply(b),
            'sleep_hours': df['Sleep_Hours_Per_Night'].apply(to_float),
            'mental_health_score': df['Mental_Health_Score'].apply(to_int),
            'relationship_status': df['Relationship_Status'].astype(str).str.strip().str[:30],
            'conflicts_over_social_media': df['Conflicts_Over_Social_Media'].apply(to_int),
            'addicted_score': df['Addicted_Score'].apply(to_int),
        })

        insert_sql = (
            """
            INSERT INTO stg_social_media_addiction (
              student_id, age, gender, academic_level, country, ave_daily_usage_hours, most_used_platform,
              affects_academic_performance, sleep_hours, mental_health_score, relationship_status,
              conflicts_over_social_media, addicted_score
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
              age=VALUES(age), gender=VALUES(gender), academic_level=VALUES(academic_level), country=VALUES(country),
              ave_daily_usage_hours=VALUES(ave_daily_usage_hours), most_used_platform=VALUES(most_used_platform),
              affects_academic_performance=VALUES(affects_academic_performance), sleep_hours=VALUES(sleep_hours),
              mental_health_score=VALUES(mental_health_score), relationship_status=VALUES(relationship_status),
              conflicts_over_social_media=VALUES(conflicts_over_social_media), addicted_score=VALUES(addicted_score)
            """
        )

        batch = []
        for row in tqdm(df2.itertuples(index=False), total=len(df2), desc='SocialMediaCSV'):
            batch.append(tuple(row))
            if len(batch) >= cfg['batch_size']:
                cursor.executemany(insert_sql, batch)
                rows_inserted += cursor.rowcount
                batch.clear()
        if batch:
            cursor.executemany(insert_sql, batch)
            rows_inserted += cursor.rowcount
        conn.commit()
        upsert_log_finish(cursor, log_id, rows_read, rows_inserted, rows_updated, 'success', None)
        conn.commit()
        print(f"Social Media Addiction CSV load complete: read={rows_read}, affected={rows_inserted}")

    except Exception as e:
        conn.rollback()
        upsert_log_finish(cursor, log_id, rows_read, rows_inserted, rows_updated, 'failed', str(e))
        conn.commit()
        print(f"Social Media Addiction CSV load failed: {e}")
        raise


def load_phq9_mendeley(conn):
    cfg = load_config()
    cursor = conn.cursor()
    dataset = 'phq9_mendeley'
    log_id = upsert_log_start(cursor, dataset)
    rows_read = rows_inserted = rows_updated = 0

    try:
        path = os.path.join(os.path.dirname(__file__), '..', 'datasets', 'mendeley-phq9-student-depression-dataset.csv')
        path = os.path.abspath(path)
        df = pd.read_csv(path)
        rows_read = len(df)

        # Source has long headers; align with target names and truncate to column limits
        rename_map = {
            'Age': 'age',
            'Gender': 'gender',
            'Little interest or pleasure in doing things?': 'has_interest_or_pleasure_in_doing_things',
            'Feeling down, depressed, or hopeless?': 'feel_down_depressed_hopeless',
            'Trouble falling or staying asleep, or sleeping too much?': 'trouble_falling_staying_asleep_sleeping_too_much',
            'Feeling tired or having little energy?': 'feel_tired_having_little_energy',
            'Poor appetite or overeating?': 'poor_appetite_overeating',
            'Feeling bad about yourself — or that you are a failure or have let yourself or your family down?': 'fell_bad_failure_let_yourself_family_down',
            'Trouble concentrating on things, such as reading the newspaper or watching television?': 'trouble_concentrating_reading_newspaper_watching_television',
            'Moving or speaking so slowly that other people could have noticed? Or the opposite — being so fidgety or restless that you have been moving around a lot more than usual?': 'move_speak_slowly_noticed_fidgety_restless',
            'Thoughts that you would be better off dead, or thoughts of hurting yourself in some way?': 'better_off_dead_hurting_yourself',
            'PHQ-9 Total Score': 'phq9_total_score',
            'Depression Level': 'depression_level',
        }
        df = df.rename(columns=rename_map)

        def trunc30(s):
            if pd.isna(s):
                return None
            return str(s).strip()[:30]

        df2 = pd.DataFrame({
            'age': df['age'].apply(to_int),
            'gender': df['gender'].apply(norm_gender),
            'has_interest_or_pleasure_in_doing_things': df['has_interest_or_pleasure_in_doing_things'].apply(trunc30),
            'feel_down_depressed_hopeless': df['feel_down_depressed_hopeless'].apply(trunc30),
            'trouble_falling_staying_asleep_sleeping_too_much': df['trouble_falling_staying_asleep_sleeping_too_much'].apply(trunc30),
            'feel_tired_having_little_energy': df['feel_tired_having_little_energy'].apply(trunc30),
            'poor_appetite_overeating': df['poor_appetite_overeating'].apply(trunc30),
            'fell_bad_failure_let_yourself_family_down': df['fell_bad_failure_let_yourself_family_down'].apply(trunc30),
            'trouble_concentrating_reading_newspaper_watching_television': df['trouble_concentrating_reading_newspaper_watching_television'].apply(trunc30),
            'move_speak_slowly_noticed_fidgety_restless': df['move_speak_slowly_noticed_fidgety_restless'].apply(trunc30),
            'better_off_dead_hurting_yourself': df['better_off_dead_hurting_yourself'].apply(trunc30),
            'phq9_total_score': df['phq9_total_score'].apply(to_int),
            'depression_level': df['depression_level'].astype(str).str.strip().str[:30],
        })
        # row hash for idempotency
        df2['row_hash'] = df2.apply(lambda r: md5_hash_fields(list(r.values)), axis=1)
        cols = ['row_hash'] + [c for c in df2.columns if c != 'row_hash']
        df2 = df2[cols]

        insert_sql = (
            """
            INSERT INTO stg_phq9_mendeley (
              row_hash, age, gender, has_interest_or_pleasure_in_doing_things, feel_down_depressed_hopeless,
              trouble_falling_staying_asleep_sleeping_too_much, feel_tired_having_little_energy, poor_appetite_overeating,
              fell_bad_failure_let_yourself_family_down, trouble_concentrating_reading_newspaper_watching_television,
              move_speak_slowly_noticed_fidgety_restless, better_off_dead_hurting_yourself, phq9_total_score, depression_level
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
              age=VALUES(age), gender=VALUES(gender), has_interest_or_pleasure_in_doing_things=VALUES(has_interest_or_pleasure_in_doing_things),
              feel_down_depressed_hopeless=VALUES(feel_down_depressed_hopeless),
              trouble_falling_staying_asleep_sleeping_too_much=VALUES(trouble_falling_staying_asleep_sleeping_too_much),
              feel_tired_having_little_energy=VALUES(feel_tired_having_little_energy),
              poor_appetite_overeating=VALUES(poor_appetite_overeating),
              fell_bad_failure_let_yourself_family_down=VALUES(fell_bad_failure_let_yourself_family_down),
              trouble_concentrating_reading_newspaper_watching_television=VALUES(trouble_concentrating_reading_newspaper_watching_television),
              move_speak_slowly_noticed_fidgety_restless=VALUES(move_speak_slowly_noticed_fidgety_restless),
              better_off_dead_hurting_yourself=VALUES(better_off_dead_hurting_yourself),
              phq9_total_score=VALUES(phq9_total_score), depression_level=VALUES(depression_level)
            """
        )

        batch = []
        for row in tqdm(df2.itertuples(index=False), total=len(df2), desc='PHQ9CSV'):
            batch.append(tuple(row))
            if len(batch) >= cfg['batch_size']:
                cursor.executemany(insert_sql, batch)
                rows_inserted += cursor.rowcount
                batch.clear()
        if batch:
            cursor.executemany(insert_sql, batch)
            rows_inserted += cursor.rowcount
        conn.commit()
        upsert_log_finish(cursor, log_id, rows_read, rows_inserted, rows_updated, 'success', None)
        conn.commit()
        print(f"PHQ9 Mendeley CSV load complete: read={rows_read}, affected={rows_inserted}")

    except Exception as e:
        conn.rollback()
        upsert_log_finish(cursor, log_id, rows_read, rows_inserted, rows_updated, 'failed', str(e))
        conn.commit()
        print(f"PHQ9 Mendeley CSV load failed: {e}")
        raise


# --------------
# CLI Entrypoint
# --------------

def init_db():
    root_conn = connect_mysql()
    cur = root_conn.cursor()
    ddl_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'sql', 'ddl_staging_mysql.sql'))
    execute_script(cur, ddl_path)
    root_conn.commit()
    cur.close()
    root_conn.close()
    print('Initialized staging database and tables.')


def run_loader(name: str):
    cfg = load_config()
    conn = connect_mysql(cfg['db_stage'])
    try:
        if name == 'cdc':
            load_cdc_indicators(conn)
        elif name == 'student_depression_csv':
            load_student_depression_csv(conn)
        elif name == 'student_mental_health_excel':
            load_student_mental_health_excel(conn)
        elif name == 'student_performance_json':
            load_student_performance_json(conn)
        elif name == 'social_media':
            load_social_media_addiction(conn)
        elif name == 'phq9':
            load_phq9_mendeley(conn)
        elif name == 'all':
            load_cdc_indicators(conn)
            load_student_depression_csv(conn)
            load_student_mental_health_excel(conn)
            load_student_performance_json(conn)
            load_social_media_addiction(conn)
            load_phq9_mendeley(conn)
        else:
            raise ValueError(f'Unknown loader: {name}')
    finally:
        conn.close()


def main(argv: List[str]):
    if len(argv) < 2:
        print('Usage: python etl.py <command>')
        print('Commands: init-db | load-cdc | load-student-depression-csv | load-student-mental-health-excel | ' \
              'load-student-performance-json | load-social-media | load-phq9 | load-all')
        return 1

    cmd = argv[1]
    if cmd == 'init-db':
        init_db()
        return 0
    elif cmd.startswith('load-'):
        name = cmd.replace('load-', '')
        run_loader(name)
        return 0
    else:
        print(f'Unknown command: {cmd}')
        return 1


if __name__ == '__main__':
    sys.exit(main(sys.argv))
