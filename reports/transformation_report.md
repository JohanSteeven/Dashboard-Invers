# Transformation Report — Phase 2

**Generated:** 2026-03-17 02:56
**Source:** `data/raw/healthcare_dataset.csv`
**Output:** `data/processed/healthcare_clean.csv`
**Diagnostic basis:** `reports/diagnosis_report.json`, `docs/01_diagnostico_tecnico.md`

---

## 1. Transformation Rules Applied

Each rule maps to a confirmed finding from Phase 1 diagnostics.

### R1 — 534 exact duplicate rows (0.96%)

- **Transformation:** Drop duplicates keeping first occurrence
- **Columns affected:** ALL
- **Technical criteria:** Exact match on all 15 columns; no PK to distinguish legitimate from erroneous
- **Dashboard impact:** Prevents double-counting in all metrics
- **Residual risk:** 5,500 suspicious near-duplicates remain; same patient+date+doctor but different fields. Kept intentionally — may be legitimate re-admissions.

### R2 — No natural primary key

- **Transformation:** Generate sequential surrogate key (admission_id)
- **Columns affected:** admission_id (new)
- **Technical criteria:** No column or compound key guarantees uniqueness across 55K rows
- **Dashboard impact:** Enables reliable row-level joins and drill-down in Metabase
- **Residual risk:** Key regenerates on re-run; not stable across exports.

### R3 — 99.94% of patient names with erratic capitalization; 216 with prefixes (Mr./Dr.)

- **Transformation:** Title Case normalization; extract prefix/suffix to separate columns
- **Columns affected:** Name, name_prefix (new), name_suffix (new)
- **Technical criteria:** Names like 'BobbY jaCKson' are unreadable in dashboards
- **Dashboard impact:** Clean display in patient-level views; prefix/suffix available for segmentation
- **Residual risk:** Names remain high-cardinality (49K+); not useful for grouping.

### R4 — 2.25% of doctor names not Title Case; 1,124 prefixes, 982 suffixes (MD/PhD/DVM)

- **Transformation:** Title Case + extract professional title to doctor_title column
- **Columns affected:** Doctor, doctor_title (new)
- **Technical criteria:** Professional titles inflate cardinality; credential info preserved separately per diagnostic recommendation
- **Dashboard impact:** Consistent doctor names for top-N and filter dropdowns; credential analysis possible via doctor_title
- **Residual risk:** Cardinality ~40K. Doctor grouping only useful for top-N queries.

### R5 — 18,635 hospital names with commas; 5,749 starting with 'and'; 5,602 ending with 'and'

- **Transformation:** Strip trailing commas, leading/trailing 'and', then Title Case
- **Columns affected:** Hospital
- **Technical criteria:** Malformed names cause fragmented GROUP BY and dirty Metabase filters
- **Dashboard impact:** Cleaner hospital filter; reduced unique hospital count
- **Residual risk:** Cardinality still ~39K; synthetic names cannot be further merged without a master list.

### R6 — Billing Amount carries up to 15 decimal digits

- **Transformation:** Round to 2 decimal places
- **Columns affected:** Billing Amount
- **Technical criteria:** Currency precision standard — cents are the minimal unit
- **Dashboard impact:** Clean SUM/AVG calculations; avoids floating-point display artifacts
- **Residual risk:** None.

### R7 — 108 records with negative billing (-2,008 to -24)

- **Transformation:** Flag with boolean is_billing_negative; do NOT delete rows
- **Columns affected:** is_billing_negative (new)
- **Technical criteria:** Negative amounts may represent adjustments/refunds — no justification column in dataset
- **Dashboard impact:** Dashboard can filter OUT negatives for revenue metrics, or IN for refund analysis
- **Residual risk:** If negatives are data errors (not refunds), they pollute billing totals unless filtered.

### R8 — 40 billing amounts below $100 in absolute value (mean $25,539)

- **Transformation:** Flag with boolean is_billing_low; do NOT delete rows
- **Columns affected:** is_billing_low (new)
- **Technical criteria:** Statistical anomalies: amounts $0.50-$99 in a dataset with mean $25K are outliers likely from generation
- **Dashboard impact:** Analysts can filter or segment these low amounts in billing distributions
- **Residual risk:** May be generation artifacts; conserved for transparency.

### R9 — Date coherence validation (discharge >= admission)

- **Transformation:** Validate and flag; Phase 1 confirmed 0 violations
- **Columns affected:** Date of Admission, Discharge Date
- **Technical criteria:** Defensive validation for future data appends
- **Dashboard impact:** Guarantees LOS and time-series integrity
- **Residual risk:** None currently. Rule serves as safeguard for incremental loads.

### R10 — Whitespace hygiene — defensive cleanup

- **Transformation:** Trim + collapse multi-spaces on all string columns
- **Columns affected:** ALL STRING
- **Technical criteria:** Standard pre-load normalization; diagnosis detected 0 issues but best practice for DB loading
- **Dashboard impact:** Prevents invisible duplicates in Metabase filters
- **Residual risk:** None.

---

## 2. Before / After Comparison

| metric                                    |   before |   after |   delta |
|:------------------------------------------|---------:|--------:|--------:|
| Total rows                                |    55500 |   54966 |    -534 |
| Exact duplicates                          |      534 |       0 |     nan |
| Total columns                             |       15 |      31 |      16 |
| Names not Title Case                      |    55467 |       0 |  -55467 |
| Hospitals ending with comma               |     4776 |       0 |   -4776 |
| Negative billing amounts                  |      108 |     106 |       0 |
| Low billing (|<$100|)                     |       40 |      40 |       0 |
| Max billing decimal places                |       15 |       2 |     nan |
| Doctors with embedded professional titles |      841 |       0 |    -841 |

---

## 3. Transformation Log

See `reports/transformation_log.csv` for the full event-level log with traceability.

**Total events logged:** 25

---

## 4. Post-Transformation Validations

| Check | Status | Detail |
|-------|--------|--------|
| V01_NO_EXACT_DUPES | PASS | 0 exact duplicates remaining |
| V02_PK_UNIQUE | PASS | admission_id unique: True |
| V03_PK_SEQUENTIAL | PASS | admission_id sequential: True |
| V04_DATES_COMPLETE | PASS | 0 NaT values in date columns |
| V05_DATE_COHERENCE | PASS | 0 rows with discharge < admission |
| V06_LOS_POSITIVE | PASS | 0 rows with stay < 1 day |
| V07_AGE_RANGE | PASS | 0 ages outside 0-120 range |
| V08_DOMAIN_Gender | PASS | All values in expected domain |
| V08_DOMAIN_Blood Type | PASS | All values in expected domain |
| V08_DOMAIN_Medical Condition | PASS | All values in expected domain |
| V08_DOMAIN_Admission Type | PASS | All values in expected domain |
| V08_DOMAIN_Medication | PASS | All values in expected domain |
| V08_DOMAIN_Test Results | PASS | All values in expected domain |
| V08_DOMAIN_Insurance Provider | PASS | All values in expected domain |
| V09_BILLING_PRECISION | PASS | Max decimal places: 2 |
| V10_NO_WHITESPACE | PASS | 0 values with leading/trailing spaces |
| V11_ROW_COUNT | PASS | 54,966 rows (expected ~54,966 after dedup) |
| V12_NAME_TITLE_CASE | PASS | 0 names not in Title Case |
| V13_HOSPITAL_NO_COMMA | PASS | 0 hospitals ending with comma |
| V14_DERIVED_COLS_PRESENT | PASS | All derived columns present |
| V15_CRITICAL_NULLS | PASS | No nulls in critical columns |
| V16_DTYPES | PASS | All dtypes correct |

**Result: 22/22 checks passed.**

---

## 5. Derived Columns

See `reports/derived_columns_dictionary.csv` for full documentation of all
derived columns added during transformation.

---

## 6. Invalid/Flagged Records

See `reports/invalid_records.csv` for all rows with quality flags.
Each row includes a `flag_reason` column indicating why it was flagged.

---

## 7. Readiness Criteria for Phase 3

The dataset is ready for SQL schema modeling and PostgreSQL loading when:

1. All validation checks above show **PASS**
2. `healthcare_clean.csv` has the expected ~54,966 rows
3. All derived columns are populated without nulls in critical fields
4. The data dictionary matches the target PostgreSQL schema
5. No unresolved FAIL validations exist
6. `transformation_log.csv` documents every change applied

---

## 8. Ambiguities and Decisions Log

| Item | Decision | Justification |
|------|----------|---------------|
| 108 negative billing amounts | **Preserved + flagged** | No column explains if refund/adjustment/error; deleting loses audit trail |
| 40 low billing amounts (<$100) | **Preserved + flagged** | Statistical outliers vs mean $25K; may be generation artifacts |
| 5,500 near-duplicate rows | **Preserved** | Different field values suggest legitimate re-admissions; cannot confirm without PK |
| Professional titles in Doctor | **Extracted to doctor_title** | Preserves credential info while cleaning name for GROUP BY |
| Uniform distributions | **Documented, no action** | Confirmed synthetic data characteristic; no correction possible |