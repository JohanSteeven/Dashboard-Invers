# Data Model Description — Healthcare Analytics

**Generated:** 2026-03-17 02:56
**Schema file:** `sql/01_schema.sql`
**Schema name:** `healthcare`

---

## 1. Model Type: Star Schema

**Justification:**

- The dataset has a single grain (one row per hospital admission)
- 5 low-cardinality categorical columns map naturally to dimensions
- Temporal analysis requires a calendar dimension (dim_date)
- High-cardinality text fields (Name, Doctor, Hospital: ~40K unique each)
  remain as degenerate dimensions on the fact table, since normalizing
  them would create dimensions nearly as large as the fact itself
- Gender (2 values) and Blood Type (8 values) use CHECK constraints
  on the fact table rather than separate dimensions, avoiding join
  overhead for minimal referential integrity gain
- A flat star schema (vs. snowflake) is optimal for Metabase, which
  performs best with minimal joins

---

## 2. Fact Table: `fact_admissions`

**Grain:** One row per hospital admission event.

**Row count:** ~54,966 (after Phase 2 deduplication)

### Measures (numeric, aggregatable)

| Column | Type | Description |
|--------|------|-------------|
| `billing_amount` | NUMERIC(12,2) | Cost billed for the admission |
| `stay_duration_days` | SMALLINT | Length of stay (discharge - admission) |
| `age` | SMALLINT | Patient age at admission |
| `room_number` | SMALLINT | Assigned room |

### Foreign Keys

| Column | References | Description |
|--------|-----------|-------------|
| `admission_date_id` | `dim_date.date_id` | Admission calendar date |
| `discharge_date_id` | `dim_date.date_id` | Discharge calendar date |
| `condition_id` | `dim_medical_condition` | Medical condition diagnosed |
| `admission_type_id` | `dim_admission_type` | Elective/Emergency/Urgent |
| `insurance_id` | `dim_insurance` | Insurance provider |
| `medication_id` | `dim_medication` | Primary medication prescribed |
| `test_result_id` | `dim_test_result` | Test result outcome |

### Degenerate Dimensions

| Column | Cardinality | Rationale for denormalization |
|--------|-------------|-------------------------------|
| `patient_name` | ~40K | No real patient ID; almost 1:1 with fact |
| `doctor_name` | ~40K | Synthetic; nearly unique per row |
| `hospital_name` | ~40K | Synthetic; nearly unique per row |
| `gender` | 2 | CHECK constraint; join cost exceeds benefit |
| `blood_type` | 8 | CHECK constraint; join cost exceeds benefit |

### Pre-computed Flags

| Column | Type | Logic | Dashboard Use |
|--------|------|-------|---------------|
| `is_pediatric` | BOOLEAN | Age < 18 | Pediatric segment filter |
| `is_billing_negative` | BOOLEAN | Billing < 0 | Exclude refunds/adjustments |
| `is_billing_low` | BOOLEAN | |Billing| < $100 | Anomaly filter |
| `is_long_stay` | BOOLEAN | LOS > P75 (23 days) | Resource analysis |
| `abnormal_test_flag` | BOOLEAN | Test = 'Abnormal' | Outcome KPI |
| `negative_outcome_flag` | BOOLEAN | Abnormal AND negative billing | Risk indicator |

---

## 3. Dimension Tables

| Dimension | Rows | Key Column | Description |
|-----------|------|------------|-------------|
| `dim_date` | ~1,856 | `date_id (YYYYMMDD)` | Calendar with year, quarter, month, day, weekday |
| `dim_medical_condition` | 6 | `condition_id` | Arthritis, Asthma, Cancer, Diabetes, Hypertension, Obesity |
| `dim_admission_type` | 3 | `admission_type_id` | Elective, Emergency, Urgent |
| `dim_insurance` | 5 | `insurance_id` | Aetna, Blue Cross, Cigna, Medicare, UnitedHealthcare |
| `dim_medication` | 5 | `medication_id` | Aspirin, Ibuprofen, Lipitor, Paracetamol, Penicillin |
| `dim_test_result` | 3 | `test_result_id` | Abnormal, Inconclusive, Normal |

---

## 4. Analytical Views

| View | Aligned Question | Joins | Description |
|------|-----------------|-------|-------------|
| `vw_monthly_admissions` | P1: Admission volume | dim_date | Monthly admissions, abnormal %, avg billing, avg LOS |
| `vw_top_hospitals` | P2: Top hospitals | dim_medical_condition | Hospital ranking by volume and billing |
| `vw_avg_los_by_condition` | P3: Average LOS | dim_medical_condition | LOS stats by condition (avg, median, P75) |
| `vw_abnormal_rate` | P4: Abnormal test % | dim_medical_condition, dim_insurance | Abnormal rate by condition x insurance |
| `vw_billing_distribution` | P5: Cost distribution | dim_insurance, dim_medical_condition | Billing by range, condition, insurance |

---

## 5. Audit Table: `load_audit`

Tracks every load operation with timestamp, source file, table name,
row count, and status. Enables traceability and re-run detection.

---

## 6. Design Decisions

| Decision | Rationale |
|----------|-----------|
| Star schema over snowflake | Single grain, no multi-level hierarchies; Metabase performs better with fewer joins |
| Degenerate dims for names | 40K unique values each; normalizing would create near-1:1 tables with no aggregation benefit |
| CHECK constraints for Gender/Blood Type | 2 and 8 values respectively; join overhead unjustified for such small domains |
| `date_id` as YYYYMMDD integer | Human-readable, sortable, efficient for range queries; avoids surrogate key lookups |
| Pre-computed flags on fact | Avoids CASE expressions in every Metabase query; negligible storage cost for 6 booleans |
| ON CONFLICT DO NOTHING | Enables idempotent re-runs without duplicates |