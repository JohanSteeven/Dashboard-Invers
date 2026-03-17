-- ============================================================
-- Healthcare Analytics — Star Schema
-- Phase 3: Data Model for PostgreSQL + Metabase
--
-- Grain: One row per hospital admission (admission_id)
-- Design: Star schema with 6 dimensions + 1 fact table
--   - 5 categorical dimensions (low cardinality: 3-6 values)
--   - 1 calendar dimension (dim_date)
--   - High-cardinality fields (Name, Doctor, Hospital) remain
--     as degenerate dimensions on the fact table
--   - Gender (2) and Blood Type (8) use CHECK constraints
--     instead of separate dimension tables
--
-- Generated from: data/processed/healthcare_clean.csv
-- ============================================================

CREATE SCHEMA IF NOT EXISTS healthcare;
SET search_path TO healthcare;

-- ============================================================
-- DIMENSION TABLES
-- ============================================================

-- Calendar dimension covering the full dataset range (2019-05-08 to 2024-06-06)
CREATE TABLE IF NOT EXISTS dim_date (
    date_id         INTEGER     PRIMARY KEY,  -- YYYYMMDD format
    full_date       DATE        NOT NULL UNIQUE,
    year            SMALLINT    NOT NULL,
    quarter         SMALLINT    NOT NULL,
    month           SMALLINT    NOT NULL,
    month_name      VARCHAR(10) NOT NULL,
    day             SMALLINT    NOT NULL,
    day_of_week     SMALLINT    NOT NULL,      -- 0=Monday, 6=Sunday
    day_name        VARCHAR(10) NOT NULL,
    is_weekend      BOOLEAN     NOT NULL
);

-- 6 medical conditions: Arthritis, Asthma, Cancer, Diabetes, Hypertension, Obesity
CREATE TABLE IF NOT EXISTS dim_medical_condition (
    condition_id    SERIAL      PRIMARY KEY,
    condition_name  VARCHAR(50) NOT NULL UNIQUE
);

-- 3 admission types: Elective, Emergency, Urgent
CREATE TABLE IF NOT EXISTS dim_admission_type (
    admission_type_id   SERIAL      PRIMARY KEY,
    admission_type_name VARCHAR(20) NOT NULL UNIQUE
);

-- 5 insurance providers: Aetna, Blue Cross, Cigna, Medicare, UnitedHealthcare
CREATE TABLE IF NOT EXISTS dim_insurance (
    insurance_id    SERIAL      PRIMARY KEY,
    provider_name   VARCHAR(50) NOT NULL UNIQUE
);

-- 5 medications: Aspirin, Ibuprofen, Lipitor, Paracetamol, Penicillin
CREATE TABLE IF NOT EXISTS dim_medication (
    medication_id   SERIAL      PRIMARY KEY,
    medication_name VARCHAR(50) NOT NULL UNIQUE
);

-- 3 test results: Abnormal, Inconclusive, Normal
CREATE TABLE IF NOT EXISTS dim_test_result (
    test_result_id  SERIAL      PRIMARY KEY,
    result_name     VARCHAR(20) NOT NULL UNIQUE
);

-- ============================================================
-- FACT TABLE
-- ============================================================

CREATE TABLE IF NOT EXISTS fact_admissions (
    -- Primary key (surrogate, generated in Phase 2)
    admission_id            INTEGER         PRIMARY KEY,

    -- Foreign keys to dimensions
    admission_date_id       INTEGER         NOT NULL REFERENCES dim_date(date_id),
    discharge_date_id       INTEGER         NOT NULL REFERENCES dim_date(date_id),
    condition_id            INTEGER         NOT NULL REFERENCES dim_medical_condition(condition_id),
    admission_type_id       INTEGER         NOT NULL REFERENCES dim_admission_type(admission_type_id),
    insurance_id            INTEGER         NOT NULL REFERENCES dim_insurance(insurance_id),
    medication_id           INTEGER         NOT NULL REFERENCES dim_medication(medication_id),
    test_result_id          INTEGER         NOT NULL REFERENCES dim_test_result(test_result_id),

    -- Degenerate dimensions: high cardinality (~40K unique each)
    patient_name            VARCHAR(100)    NOT NULL,
    name_prefix             VARCHAR(10),
    name_suffix             VARCHAR(10),
    doctor_name             VARCHAR(100)    NOT NULL,
    doctor_title            VARCHAR(10),
    hospital_name           VARCHAR(100)    NOT NULL,

    -- Degenerate dimensions: low cardinality with CHECK constraints
    gender                  VARCHAR(10)     NOT NULL,
    blood_type              VARCHAR(5)      NOT NULL,

    -- Measures
    age                     SMALLINT        NOT NULL,
    room_number             SMALLINT        NOT NULL,
    billing_amount          NUMERIC(12,2)   NOT NULL,
    stay_duration_days      SMALLINT        NOT NULL,

    -- Pre-computed derived categoricals (for Metabase convenience)
    age_group               VARCHAR(10)     NOT NULL,
    billing_range           VARCHAR(15)     NOT NULL,

    -- Pre-computed flags
    is_pediatric            BOOLEAN         NOT NULL DEFAULT FALSE,
    is_billing_negative     BOOLEAN         NOT NULL DEFAULT FALSE,
    is_billing_low          BOOLEAN         NOT NULL DEFAULT FALSE,
    is_long_stay            BOOLEAN         NOT NULL DEFAULT FALSE,
    abnormal_test_flag      BOOLEAN         NOT NULL DEFAULT FALSE,
    negative_outcome_flag   BOOLEAN         NOT NULL DEFAULT FALSE,

    -- Domain integrity constraints
    CONSTRAINT chk_gender
        CHECK (gender IN ('Male', 'Female')),
    CONSTRAINT chk_blood_type
        CHECK (blood_type IN ('A+','A-','B+','B-','AB+','AB-','O+','O-')),
    CONSTRAINT chk_age
        CHECK (age BETWEEN 0 AND 120),
    CONSTRAINT chk_stay_positive
        CHECK (stay_duration_days >= 1),
    CONSTRAINT chk_billing_range
        CHECK (billing_range IN ('Low','Medium','High','Very High')),
    CONSTRAINT chk_age_group
        CHECK (age_group IN ('13-17','18-30','31-45','46-60','61-75','76-89')),
    CONSTRAINT chk_discharge_after_admission
        CHECK (discharge_date_id >= admission_date_id)
);

-- ============================================================
-- INDEXES (optimized for Metabase queries)
-- ============================================================

-- FK join indexes
CREATE INDEX IF NOT EXISTS idx_fact_adm_date       ON fact_admissions(admission_date_id);
CREATE INDEX IF NOT EXISTS idx_fact_dis_date       ON fact_admissions(discharge_date_id);
CREATE INDEX IF NOT EXISTS idx_fact_condition       ON fact_admissions(condition_id);
CREATE INDEX IF NOT EXISTS idx_fact_adm_type        ON fact_admissions(admission_type_id);
CREATE INDEX IF NOT EXISTS idx_fact_insurance       ON fact_admissions(insurance_id);
CREATE INDEX IF NOT EXISTS idx_fact_medication      ON fact_admissions(medication_id);
CREATE INDEX IF NOT EXISTS idx_fact_test_result     ON fact_admissions(test_result_id);

-- Filter/group-by indexes for top-N and drill-down queries
CREATE INDEX IF NOT EXISTS idx_fact_hospital        ON fact_admissions(hospital_name);
CREATE INDEX IF NOT EXISTS idx_fact_gender          ON fact_admissions(gender);
CREATE INDEX IF NOT EXISTS idx_fact_age_group       ON fact_admissions(age_group);
CREATE INDEX IF NOT EXISTS idx_fact_billing_range   ON fact_admissions(billing_range);

-- Measure range scans
CREATE INDEX IF NOT EXISTS idx_fact_billing_amt     ON fact_admissions(billing_amount);
CREATE INDEX IF NOT EXISTS idx_fact_los             ON fact_admissions(stay_duration_days);

-- ============================================================
-- AUDIT TABLE
-- ============================================================

CREATE TABLE IF NOT EXISTS load_audit (
    load_id         SERIAL      PRIMARY KEY,
    loaded_at       TIMESTAMP   NOT NULL DEFAULT NOW(),
    source_file     VARCHAR(500)NOT NULL,
    table_name      VARCHAR(100)NOT NULL,
    rows_loaded     INTEGER     NOT NULL,
    status          VARCHAR(20) NOT NULL DEFAULT 'SUCCESS',
    detail          TEXT
);

-- ============================================================
-- ANALYTICAL VIEWS (aligned with 5 dashboard questions)
-- ============================================================

-- P1: Monthly admission volume with key metrics
CREATE OR REPLACE VIEW vw_monthly_admissions AS
SELECT
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    d.year || '-' || LPAD(d.month::TEXT, 2, '0') AS year_month,
    COUNT(*)                                        AS admission_count,
    COUNT(*) FILTER (WHERE f.abnormal_test_flag)    AS abnormal_count,
    ROUND(100.0 * COUNT(*) FILTER (WHERE f.abnormal_test_flag)
          / NULLIF(COUNT(*), 0), 2)                 AS abnormal_pct,
    ROUND(AVG(f.billing_amount), 2)                 AS avg_billing,
    ROUND(AVG(f.stay_duration_days), 1)             AS avg_los,
    COUNT(*) FILTER (WHERE f.is_pediatric)          AS pediatric_count
FROM fact_admissions f
JOIN dim_date d ON f.admission_date_id = d.date_id
GROUP BY d.year, d.quarter, d.month, d.month_name
ORDER BY d.year, d.month;

-- P2: Top hospitals by admissions and billing
CREATE OR REPLACE VIEW vw_top_hospitals AS
SELECT
    f.hospital_name,
    COUNT(*)                                        AS admission_count,
    ROUND(SUM(f.billing_amount), 2)                 AS total_billing,
    ROUND(AVG(f.billing_amount), 2)                 AS avg_billing,
    ROUND(AVG(f.stay_duration_days), 1)             AS avg_los,
    COUNT(*) FILTER (WHERE f.abnormal_test_flag)    AS abnormal_count,
    COUNT(DISTINCT mc.condition_name)               AS conditions_treated
FROM fact_admissions f
JOIN dim_medical_condition mc ON f.condition_id = mc.condition_id
WHERE NOT f.is_billing_negative
GROUP BY f.hospital_name
ORDER BY admission_count DESC;

-- P3: Average LOS by medical condition
CREATE OR REPLACE VIEW vw_avg_los_by_condition AS
SELECT
    mc.condition_name,
    COUNT(*)                                        AS admission_count,
    ROUND(AVG(f.stay_duration_days), 2)             AS avg_los,
    MIN(f.stay_duration_days)                       AS min_los,
    MAX(f.stay_duration_days)                       AS max_los,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP
          (ORDER BY f.stay_duration_days)::NUMERIC, 1) AS median_los,
    COUNT(*) FILTER (WHERE f.is_long_stay)          AS long_stay_count,
    ROUND(100.0 * COUNT(*) FILTER (WHERE f.is_long_stay)
          / NULLIF(COUNT(*), 0), 2)                 AS long_stay_pct
FROM fact_admissions f
JOIN dim_medical_condition mc ON f.condition_id = mc.condition_id
GROUP BY mc.condition_name
ORDER BY avg_los DESC;

-- P4: Abnormal test rate by condition and insurance
CREATE OR REPLACE VIEW vw_abnormal_rate AS
SELECT
    mc.condition_name,
    ip.provider_name,
    COUNT(*)                                        AS total_admissions,
    COUNT(*) FILTER (WHERE f.abnormal_test_flag)    AS abnormal_count,
    ROUND(100.0 * COUNT(*) FILTER (WHERE f.abnormal_test_flag)
          / NULLIF(COUNT(*), 0), 2)                 AS abnormal_pct
FROM fact_admissions f
JOIN dim_medical_condition mc ON f.condition_id = mc.condition_id
JOIN dim_insurance ip ON f.insurance_id = ip.insurance_id
GROUP BY mc.condition_name, ip.provider_name
ORDER BY mc.condition_name, abnormal_pct DESC;

-- P5: Billing distribution by insurance and condition
CREATE OR REPLACE VIEW vw_billing_distribution AS
SELECT
    ip.provider_name,
    mc.condition_name,
    f.billing_range,
    COUNT(*)                                        AS admission_count,
    ROUND(SUM(f.billing_amount), 2)                 AS total_billing,
    ROUND(AVG(f.billing_amount), 2)                 AS avg_billing,
    ROUND(MIN(f.billing_amount), 2)                 AS min_billing,
    ROUND(MAX(f.billing_amount), 2)                 AS max_billing
FROM fact_admissions f
JOIN dim_insurance ip ON f.insurance_id = ip.insurance_id
JOIN dim_medical_condition mc ON f.condition_id = mc.condition_id
WHERE NOT f.is_billing_negative
GROUP BY ip.provider_name, mc.condition_name, f.billing_range
ORDER BY ip.provider_name, mc.condition_name, f.billing_range;
