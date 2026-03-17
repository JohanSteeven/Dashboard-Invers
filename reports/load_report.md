# Load Report — Phase 3

**Generated:** 2026-03-17 02:56
**Source:** `data/processed/healthcare_clean.csv`
**Target:** PostgreSQL schema `healthcare`

---

## 1. Load Summary

| Table | Rows Loaded | Status | Detail |
|-------|-------------|--------|--------|
| dim_date | 1,857 | OK | 2019-05-08 to 2024-06-06 (1857 days) |
| dim_medical_condition | 6 | OK |  |
| dim_admission_type | 3 | OK |  |
| dim_insurance | 5 | OK |  |
| dim_medication | 5 | OK |  |
| dim_test_result | 3 | OK |  |
| fact_admissions | 54,966 | OK |  |

**Total rows loaded across all tables: 56,845**

---

## 2. Post-Load Validations

| Check | Status | Detail |
|-------|--------|--------|
| V1_FACT_COUNT | PASS | 54,966 rows in fact_admissions |
| V2_DIM_DATE | PASS | 1857 rows in dim_date |
| V2_DIM_MEDICAL_CONDITION | PASS | 6 rows in dim_medical_condition |
| V2_DIM_ADMISSION_TYPE | PASS | 3 rows in dim_admission_type |
| V2_DIM_INSURANCE | PASS | 5 rows in dim_insurance |
| V2_DIM_MEDICATION | PASS | 5 rows in dim_medication |
| V2_DIM_TEST_RESULT | PASS | 3 rows in dim_test_result |
| V3_FK_INTEGRITY | PASS | 0 orphan condition FKs |
| V4_DATE_RANGE | PASS | Admission dates: 2019-05-08 to 2024-05-07 |
| V5_NO_DUP_PK | PASS | 0 duplicate admission_ids |
| V6_VW_MONTHLY_ADMISSIONS | PASS | 61 rows |
| V6_VW_TOP_HOSPITALS | PASS | 39575 rows |
| V6_VW_AVG_LOS_BY_CONDITION | PASS | 6 rows |
| V6_VW_ABNORMAL_RATE | PASS | 30 rows |
| V6_VW_BILLING_DISTRIBUTION | PASS | 120 rows |
| V7_BILLING_SANITY | PASS | Total billing (excl. negative): $1,404,121,599.97, Avg: $25,594.63 |

**Result: 16/16 checks passed.**

---

## 3. Rejected Records

**0 rows rejected.**

---

## 4. Readiness for Phase 4 (Dashboard)

The database is ready for Metabase dashboard construction when:

1. All validation checks above show **PASS**
2. All 5 analytical views return data
3. `fact_admissions` row count matches expected ~54,966
4. FK integrity is confirmed (0 orphans)
5. Billing totals are in expected range