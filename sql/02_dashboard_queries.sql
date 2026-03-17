-- ============================================================
-- Healthcare Analytics — Dashboard Queries for Metabase
-- Phase 4: Visualization Layer
--
-- All queries use schema: healthcare
-- Compatible with PostgreSQL 13+
-- Optimized for Metabase native queries
-- ============================================================

SET search_path TO healthcare;

-- ============================================================
-- KPI SCORECARDS (Row 1)
-- ============================================================

-- KPI 1: Total Admissions
-- Type: Number (Scorecard)
-- Expected: ~54,966
SELECT COUNT(*) AS total_admissions
FROM fact_admissions;


-- KPI 2: Total Billing Revenue (excluding negative billing)
-- Type: Number (Scorecard), format: currency USD
-- Expected: ~$1.4B
SELECT SUM(billing_amount) AS total_billing
FROM fact_admissions
WHERE NOT is_billing_negative;


-- KPI 3: Abnormal Test Rate (%)
-- Type: Number (Scorecard), suffix: %
-- Expected: ~33.6%
SELECT
    ROUND(100.0 * COUNT(*) FILTER (WHERE abnormal_test_flag)
          / COUNT(*), 1) AS abnormal_rate_pct
FROM fact_admissions;


-- KPI 4: Average Length of Stay (days)
-- Type: Number (Scorecard), suffix: days
-- Expected: ~15.5
SELECT ROUND(AVG(stay_duration_days), 1) AS avg_los_days
FROM fact_admissions;


-- ============================================================
-- Q1: MONTHLY ADMISSION VOLUME (Row 2)
-- ============================================================
-- Type: Line chart (full width)
-- X-axis: year_month | Y-axis: admission_count
-- Insight: Stable ~910/month, drops at range boundaries

SELECT
    d.year || '-' || LPAD(d.month::TEXT, 2, '0')    AS year_month,
    COUNT(*)                                          AS admission_count,
    ROUND(AVG(f.billing_amount), 2)                   AS avg_billing,
    ROUND(AVG(f.stay_duration_days), 1)                AS avg_los
FROM fact_admissions f
JOIN dim_date d ON f.admission_date_id = d.date_id
GROUP BY d.year, d.month
ORDER BY d.year, d.month;


-- ============================================================
-- Q2: TOP 10 HOSPITALS BY TOTAL BILLING (Row 3 left)
-- ============================================================
-- Type: Horizontal bar chart
-- X-axis: total_billing | Y-axis: hospital_name
-- Insight: High cardinality (~40K hospitals), top 10 driven by random variance

SELECT
    f.hospital_name,
    COUNT(*)                            AS admission_count,
    ROUND(SUM(f.billing_amount), 2)     AS total_billing,
    ROUND(AVG(f.billing_amount), 2)     AS avg_billing,
    ROUND(AVG(f.stay_duration_days), 1) AS avg_los
FROM fact_admissions f
WHERE NOT f.is_billing_negative
GROUP BY f.hospital_name
ORDER BY total_billing DESC
LIMIT 10;


-- ============================================================
-- Q5: AVG BILLING BY INSURANCE & CONDITION (Row 3 right)
-- ============================================================
-- Type: Grouped bar chart
-- X-axis: provider_name | Y-axis: avg_billing
-- Series: condition_name
-- Insight: All averages converge to ~$25,500

SELECT
    ip.provider_name,
    mc.condition_name,
    COUNT(*)                          AS admission_count,
    ROUND(AVG(f.billing_amount), 2)   AS avg_billing
FROM fact_admissions f
JOIN dim_insurance ip
    ON f.insurance_id = ip.insurance_id
JOIN dim_medical_condition mc
    ON f.condition_id = mc.condition_id
WHERE NOT f.is_billing_negative
GROUP BY ip.provider_name, mc.condition_name
ORDER BY ip.provider_name, avg_billing DESC;


-- ============================================================
-- Q3: AVG & MEDIAN LOS BY CONDITION (Row 4 left)
-- ============================================================
-- Type: Grouped bar chart (avg + median as two series)
-- X-axis: condition_name | Y-axis: avg_los, median_los
-- Insight: All conditions ~15.5 days, ~25% long stay

SELECT
    mc.condition_name,
    COUNT(*)                                            AS admission_count,
    ROUND(AVG(f.stay_duration_days), 2)                 AS avg_los,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP
          (ORDER BY f.stay_duration_days), 1)            AS median_los,
    MIN(f.stay_duration_days)                            AS min_los,
    MAX(f.stay_duration_days)                            AS max_los,
    COUNT(*) FILTER (WHERE f.is_long_stay)               AS long_stay_count,
    ROUND(100.0 * COUNT(*) FILTER (WHERE f.is_long_stay)
          / NULLIF(COUNT(*), 0), 1)                      AS long_stay_pct
FROM fact_admissions f
JOIN dim_medical_condition mc
    ON f.condition_id = mc.condition_id
GROUP BY mc.condition_name
ORDER BY avg_los DESC;


-- ============================================================
-- Q4: ABNORMAL TEST RATE BY CONDITION & INSURANCE (Row 4 right)
-- ============================================================
-- Type: Pivot table / Heatmap
-- Rows: condition_name | Columns: provider_name | Values: abnormal_pct
-- Insight: All cells ~33%, uniform distribution

SELECT
    mc.condition_name,
    ip.provider_name,
    COUNT(*)                                            AS total_admissions,
    COUNT(*) FILTER (WHERE f.abnormal_test_flag)        AS abnormal_count,
    ROUND(100.0 * COUNT(*) FILTER (WHERE f.abnormal_test_flag)
          / NULLIF(COUNT(*), 0), 1)                     AS abnormal_pct
FROM fact_admissions f
JOIN dim_medical_condition mc
    ON f.condition_id = mc.condition_id
JOIN dim_insurance ip
    ON f.insurance_id = ip.insurance_id
GROUP BY mc.condition_name, ip.provider_name
ORDER BY mc.condition_name, abnormal_pct DESC;


-- ============================================================
-- EXTRA: ADMISSIONS BY AGE GROUP & ADMISSION TYPE (Row 5)
-- ============================================================
-- Type: Stacked bar chart (full width)
-- X-axis: age_group | Y-axis: admission_count
-- Series: admission_type_name
-- Insight: Pediatric group (13-17) has ~116 admissions vs ~9K-12K for adult groups

SELECT
    at.admission_type_name,
    f.age_group,
    COUNT(*) AS admission_count
FROM fact_admissions f
JOIN dim_admission_type at
    ON f.admission_type_id = at.admission_type_id
GROUP BY at.admission_type_name, f.age_group
ORDER BY
    CASE f.age_group
        WHEN '13-17' THEN 1
        WHEN '18-30' THEN 2
        WHEN '31-45' THEN 3
        WHEN '46-60' THEN 4
        WHEN '61-75' THEN 5
        WHEN '76-89' THEN 6
    END,
    at.admission_type_name;
