# EDA Report — Healthcare Dataset
**Generated:** 2026-03-17 02:56
**Source:** `data/raw/healthcare_dataset.csv`

---

## 1. Structural Overview

- **Rows:** 55,500
- **Columns:** 15
- **Memory:** 34.54 MB

### Column Types

| Column | Type |
|--------|------|
| Name | `str` |
| Age | `int64` |
| Gender | `str` |
| Blood Type | `str` |
| Medical Condition | `str` |
| Date of Admission | `datetime64[us]` |
| Doctor | `str` |
| Hospital | `str` |
| Insurance Provider | `str` |
| Billing Amount | `float64` |
| Room Number | `int64` |
| Admission Type | `str` |
| Discharge Date | `datetime64[us]` |
| Medication | `str` |
| Test Results | `str` |

---

## 2. Null Analysis

**Total nulls:** 0

| Column | Null Count | Null % |
|--------|-----------|--------|
| Name | 0 | 0.0% |
| Age | 0 | 0.0% |
| Gender | 0 | 0.0% |
| Blood Type | 0 | 0.0% |
| Medical Condition | 0 | 0.0% |
| Date of Admission | 0 | 0.0% |
| Doctor | 0 | 0.0% |
| Hospital | 0 | 0.0% |
| Insurance Provider | 0 | 0.0% |
| Billing Amount | 0 | 0.0% |
| Room Number | 0 | 0.0% |
| Admission Type | 0 | 0.0% |
| Discharge Date | 0 | 0.0% |
| Medication | 0 | 0.0% |
| Test Results | 0 | 0.0% |

---

## 3. Duplicate Analysis

- **Exact duplicates:** 534 (0.96%)
- **Suspicious near-duplicates:** 5500
- **Rows after dedup:** 54,966

---

## 4. Numerical Profiling

| column         |   count |     mean |      std |      min |      q1 |   median |      q3 |     max |     iqr |   lower_fence |   upper_fence |   outlier_count |   outlier_pct |   negative_values |
|:---------------|--------:|---------:|---------:|---------:|--------:|---------:|--------:|--------:|--------:|--------------:|--------------:|----------------:|--------------:|------------------:|
| Age            |   55500 |    51.54 |    19.6  |    13    |    35   |     52   |    68   |    89   |    33   |         -14.5 |         117.5 |               0 |             0 |                 0 |
| Billing Amount |   55500 | 25539.3  | 14211.5  | -2008.49 | 13241.2 |  25538.1 | 37820.5 | 52764.3 | 24579.3 |      -23627.7 |       74689.4 |               0 |             0 |               108 |
| Room Number    |   55500 |   301.13 |   115.24 |   101    |   202   |    302   |   401   |   500   |   199   |         -96.5 |         699.5 |               0 |             0 |                 0 |

---

## 5. Date Analysis

### Date of Admission
- **nat_count:** 0
- **min:** 2019-05-08
- **max:** 2024-05-07
- **unique_dates:** 1827

### Discharge Date
- **nat_count:** 0
- **min:** 2019-05-09
- **max:** 2024-06-06
- **unique_dates:** 1856

- **discharge_before_admission:** 0
### length_of_stay
- **min:** 1
- **max:** 30
- **mean:** 15.51
- **zero_day_stays:** 0


---

## 6. Text Quality

### Name
- **total:** 55,500
- **leading_trailing_spaces:** 0
- **multiple_spaces:** 0
- **not_title_case:** 55,467
- **not_title_case_pct:** 99.94
- **with_prefix:** 216

### Doctor
- **total:** 55,500
- **leading_trailing_spaces:** 0
- **multiple_spaces:** 0
- **not_title_case:** 1,246
- **not_title_case_pct:** 2.25
- **with_suffix:** 982
- **with_prefix:** 1,124

### Hospital
- **total:** 55,500
- **leading_trailing_spaces:** 0
- **multiple_spaces:** 0
- **not_title_case:** 27,859
- **not_title_case_pct:** 50.2
- **contains_comma:** 18,635
- **starts_with_and:** 5,749
- **ends_with_and:** 5,602
- **ends_with_comma:** 4,776

---

## 7. Quality Issues (Prioritized)

| # | Severity | Issue | Detail | Action |
|---|----------|-------|--------|--------|
| 1 | HIGH | Exact duplicate rows | 534 rows (0.96%) | Remove exact duplicates |
| 2 | HIGH | No natural primary key | No column or combination guarantees uniqueness | Generate surrogate key (admission_id) |
| 3 | OBSERVATION | Zero nulls in all columns | Suspiciously clean for a 55K-row dataset — consistent with synthetic data | Document as synthetic data indicator |
| 4 | MEDIUM | Erratic capitalization in patient names | 55467 names (99.94%) | Normalize to Title Case, remove prefixes |
| 5 | MEDIUM | Negative billing amounts | 108 records with negative billing | Flag with is_billing_negative; exclude from revenue metrics |
| 6 | MEDIUM | Malformed hospital names | 18635 with commas, 5749 starting with 'and' | Clean trailing commas, leading/trailing 'and' |
| 7 | LOW | Excessive decimal precision in Billing Amount | Up to 15 decimals in billing values | Round to 2 decimals |
| 9 | OBSERVATION | Perfectly uniform distributions in all categorical columns | Consistent with synthetically generated data | Document; comparative analysis between categories will not yield significant differences |

---

## 8. Visualizations

See `reports/figures/` for generated charts:
- `null_distribution.png`
- `top_categories_medical_condition.png`
- `distribution_billing_amount.png`
- `monthly_admissions.png`