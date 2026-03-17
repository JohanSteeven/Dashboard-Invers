"""
Phase 2 — Limpieza y Transformacion del Healthcare Dataset

Every transformation rule is derived from confirmed Phase 1 findings:
  - diagnosis_report.json (8 quality issues)
  - 01_diagnostico_tecnico.md (10 prioritized problems)
  - numerical_summary.csv (108 negative billing amounts, 40 below $100)
  - categorical_summary.csv (categories confirmed clean)
  - data_dictionary.csv (types/cardinality mapped)

Outputs:
  data/processed/healthcare_clean.csv
  reports/transformation_report.md
  reports/transformation_log.csv
  reports/data_quality_before_after.csv
  reports/invalid_records.csv
  reports/derived_columns_dictionary.csv
"""

import logging
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_RAW = PROJECT_ROOT / "data" / "raw" / "healthcare_dataset.csv"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"
REPORTS_DIR = PROJECT_ROOT / "reports"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("phase2")

# ---------------------------------------------------------------------------
# Transformation log accumulator
# ---------------------------------------------------------------------------
_transform_log: list[dict] = []


def _log_event(
    rule: str,
    column: str,
    rows_affected: int,
    detail: str = "",
    criteria: str = "",
    risk: str = "",
) -> None:
    """Append a transformation event to the global log with full traceability."""
    _transform_log.append({
        "rule": rule,
        "column": column,
        "rows_affected": rows_affected,
        "detail": detail,
        "criteria": criteria,
        "residual_risk": risk,
    })
    log.info("[%s] %s: %s rows — %s", rule, column, f"{rows_affected:,}", detail)


# ===================================================================
# LOAD
# ===================================================================
def load_raw(path: Path) -> pd.DataFrame:
    """Load the raw CSV with date parsing and encoding handling."""
    if not path.exists():
        log.error("File not found: %s", path)
        sys.exit(1)

    df = pd.read_csv(path, encoding="utf-8")
    log.info("Loaded %s rows x %s columns from %s", f"{len(df):,}", len(df.columns), path.name)

    for col in ["Date of Admission", "Discharge Date"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")
        nat_count = int(df[col].isna().sum())
        if nat_count > 0:
            log.warning("%s: %d unparseable dates coerced to NaT", col, nat_count)

    return df


# ===================================================================
# RULE 1 — Remove exact duplicates
# Diagnosis: 534 exact duplicate rows (0.96%), Issue #1 HIGH
# ===================================================================
def remove_exact_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Drop exact duplicate rows, keeping the first occurrence."""
    before = len(df)
    df = df.drop_duplicates(keep="first").reset_index(drop=True)
    removed = before - len(df)
    _log_event(
        "R1_DEDUP", "ALL", removed,
        f"Removed exact duplicates: {before:,} -> {len(df):,} rows",
        "Exact match on all 15 columns",
        "5,500 near-duplicates remain; may be legitimate re-admissions",
    )
    return df


# ===================================================================
# RULE 2 — Generate surrogate primary key
# Diagnosis: No natural column guarantees uniqueness, Issue #2 HIGH
# ===================================================================
def add_surrogate_key(df: pd.DataFrame) -> pd.DataFrame:
    """Add an integer surrogate key as the first column."""
    df.insert(0, "admission_id", range(1, len(df) + 1))
    _log_event(
        "R2_PK", "admission_id", len(df),
        "Surrogate key generated (1..N after dedup)",
        "No natural PK exists in dataset",
        "Key regenerates on re-run; not stable across exports",
    )
    return df


# ===================================================================
# RULE 3 — Normalize patient names
# Diagnosis: 99.94% not Title Case (55,467), 216 with prefixes
#            Issue #3 MEDIUM
# ===================================================================
_NAME_PREFIXES = re.compile(r"^(Mr\.?|Mrs\.?|Ms\.?|Dr\.?|Miss)\s+", re.IGNORECASE)
_NAME_SUFFIXES = re.compile(r"\s+(Jr\.?|Sr\.?|II|III|IV)$", re.IGNORECASE)


def normalize_names(df: pd.DataFrame) -> pd.DataFrame:
    """Title-case patient names, extract prefixes/suffixes to separate columns."""
    raw = df["Name"].astype(str)

    # Extract prefixes and suffixes before removing
    df["name_prefix"] = raw.str.extract(_NAME_PREFIXES.pattern, flags=re.IGNORECASE)[0]
    df["name_suffix"] = raw.str.extract(
        r"\s+(Jr\.?|Sr\.?|II|III|IV)$", flags=re.IGNORECASE
    )[0]

    prefixes_found = int(df["name_prefix"].notna().sum())
    suffixes_found = int(df["name_suffix"].notna().sum())

    # Clean: remove prefix, remove suffix, title-case
    cleaned = raw.str.replace(_NAME_PREFIXES, "", regex=True)
    cleaned = cleaned.str.replace(_NAME_SUFFIXES, "", regex=True)
    cleaned = cleaned.str.strip().str.title()

    changed = int((cleaned != raw).sum())
    df["Name"] = cleaned
    _log_event("R3_NAME", "Name", changed,
               "Normalized to Title Case",
               "99.94% had erratic capitalization",
               "Names remain high-cardinality (49K+); not useful for grouping")
    _log_event("R3_PREFIX", "name_prefix", prefixes_found,
               f"Extracted {prefixes_found} prefixes (Mr., Dr., etc.)",
               "2% of records had embedded titles")
    _log_event("R3_SUFFIX", "name_suffix", suffixes_found,
               f"Extracted {suffixes_found} suffixes (Jr., Sr., etc.)")
    return df


# ===================================================================
# RULE 4 — Normalize doctor names + extract title
# Diagnosis: 2.25% not Title Case, 1,124 prefixes, 982 suffixes
#            Issue #7 LOW, proposed doctor_title column
# ===================================================================
_DOCTOR_SUFFIX = re.compile(r"\s+(MD|DVM|PhD|Jr\.?|Sr\.?)$", re.IGNORECASE)


def normalize_doctors(df: pd.DataFrame) -> pd.DataFrame:
    """Title-case doctor names, extract professional title to separate column."""
    raw = df["Doctor"].astype(str)

    # Extract professional suffix before removing
    df["doctor_title"] = raw.str.extract(_DOCTOR_SUFFIX.pattern, flags=re.IGNORECASE)[0]
    titles_found = int(df["doctor_title"].notna().sum())

    # Remove prefix titles and suffixes
    cleaned = raw.str.replace(_NAME_PREFIXES, "", regex=True)
    cleaned = cleaned.str.replace(_DOCTOR_SUFFIX, "", regex=True)
    cleaned = cleaned.str.strip().str.title()

    changed = int((cleaned != raw).sum())
    df["Doctor"] = cleaned
    _log_event("R4_DOCTOR", "Doctor", changed,
               "Title Case + removed titles/suffixes",
               "2.25% not Title Case; 1,124 prefixes, 982 suffixes",
               "Cardinality ~40K; doctor grouping only useful for top-N queries")
    _log_event("R4_TITLE", "doctor_title", titles_found,
               f"Extracted {titles_found} professional titles (MD, PhD, DVM, etc.)",
               "Preserves clinical credential info from diagnosis")
    return df


# ===================================================================
# RULE 5 — Clean hospital names
# Diagnosis: 18,635 with commas, 5,749 starting with 'and',
#            5,602 ending with 'and', 4,776 ending with comma
#            Issue #5 MEDIUM
# ===================================================================
def clean_hospital_names(df: pd.DataFrame) -> pd.DataFrame:
    """Remove trailing commas, leading/trailing 'and', normalize case."""
    raw = df["Hospital"].astype(str)
    cleaned = raw.copy()

    # Strip trailing commas (multiple passes for nested cases)
    cleaned = cleaned.str.replace(r",\s*$", "", regex=True)
    # Remove leading "and "
    cleaned = cleaned.str.replace(r"^and\s+", "", regex=True, flags=re.IGNORECASE)
    # Remove trailing " and"
    cleaned = cleaned.str.replace(r"\s+and$", "", regex=True, flags=re.IGNORECASE)
    # Clean remaining loose commas
    cleaned = cleaned.str.replace(r",\s*,", ",", regex=True)
    cleaned = cleaned.str.replace(r",\s*$", "", regex=True)
    # Title Case
    cleaned = cleaned.str.strip().str.title()

    changed = int((cleaned != raw).sum())
    df["Hospital"] = cleaned
    _log_event("R5_HOSPITAL", "Hospital", changed,
               "Removed commas/leading-trailing 'and', Title Case",
               "42% malformed: commas, 'and' artifacts from faker generation",
               "Cardinality still ~39K; synthetic names cannot be merged without master list")
    return df


# ===================================================================
# RULE 6 — Round billing to 2 decimals
# Diagnosis: Up to 15 decimal places, Issue #6 LOW
# ===================================================================
def round_billing(df: pd.DataFrame) -> pd.DataFrame:
    """Round Billing Amount to 2 decimal places (currency standard)."""
    df["Billing Amount"] = df["Billing Amount"].round(2)
    _log_event("R6_BILLING", "Billing Amount", len(df),
               "Rounded to 2 decimals",
               "Up to 15 decimal digits detected in diagnosis",
               "None")
    return df


# ===================================================================
# RULE 7 — Flag negative billing (do NOT delete)
# Diagnosis: 108 records with negative billing (-2,008.49 to -23.87)
#            Issue #4 MEDIUM
# ===================================================================
def flag_negative_billing(df: pd.DataFrame) -> pd.DataFrame:
    """Add boolean flag for negative billing amounts."""
    mask = df["Billing Amount"] < 0
    df["is_billing_negative"] = mask
    count = int(mask.sum())
    _log_event("R7_NEG_BILLING", "is_billing_negative", count,
               "Flagged negative amounts; rows preserved for audit trail",
               "108 records: may be adjustments/refunds — no justification column exists",
               "If errors (not refunds), they pollute totals unless filtered")
    return df


# ===================================================================
# RULE 8 — Flag low billing amounts
# Diagnosis: 40 amounts < $100 in absolute value, Issue #8 LOW
#            Mean $25,539; amounts like $0.50-$99 are statistical anomalies
# ===================================================================
def flag_low_billing(df: pd.DataFrame) -> pd.DataFrame:
    """Flag billing amounts below $100 in absolute value."""
    mask = df["Billing Amount"].abs() < 100
    df["is_billing_low"] = mask
    count = int(mask.sum())
    _log_event("R8_LOW_BILLING", "is_billing_low", count,
               "Flagged amounts |<$100| for analyst review",
               "40 amounts anomalously low vs mean $25,539; statistical outliers",
               "May be generation artifacts; conserved for transparency")
    return df


# ===================================================================
# RULE 9 — Validate date coherence
# Diagnosis: 0 discharge < admission, LOS 1-30, 0 zero-day stays
#            Defensive validation for future data appends
# ===================================================================
def validate_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Validate admission/discharge coherence and flag violations."""
    invalid_mask = df["Discharge Date"] < df["Date of Admission"]
    invalid_count = int(invalid_mask.sum())

    nat_adm = int(df["Date of Admission"].isna().sum())
    nat_dis = int(df["Discharge Date"].isna().sum())

    _log_event("R9_DATE", "Date of Admission", nat_adm,
               f"NaT values in admission dates",
               "Defensive check; diagnosis found 0 NaT")
    _log_event("R9_DATE", "Discharge Date", nat_dis,
               f"NaT values in discharge dates")
    _log_event("R9_COHERENCE", "dates", invalid_count,
               f"Incoherent date pairs (discharge < admission)",
               "Diagnosis confirmed 0 violations; rule serves as safeguard",
               "None currently")

    if invalid_count > 0:
        df.loc[invalid_mask, "date_quality_flag"] = "discharge_before_admission"
        log.warning("%d rows with discharge before admission — flagged", invalid_count)
    return df


# ===================================================================
# RULE 10 — Trim & collapse whitespace on all string columns
# Diagnosis: 0 leading/trailing spaces detected, but standard
#            pre-load normalization for DB ingest
# ===================================================================
def clean_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    """Trim and collapse multi-spaces on all object/string columns."""
    str_cols = df.select_dtypes(include=["object", "string"]).columns.tolist()
    total_changed = 0
    for col in str_cols:
        notna_mask = df[col].notna()
        original = df.loc[notna_mask, col].copy()
        cleaned = original.str.strip().str.replace(r"\s{2,}", " ", regex=True)
        changed = int((cleaned != original).sum())
        total_changed += changed
        df.loc[notna_mask, col] = cleaned
    _log_event("R10_WHITESPACE", "ALL_STRING", total_changed,
               "Trim + collapse multi-spaces",
               "Defensive; diagnosis found 0 issues but standard practice for DB load",
               "None")
    return df


# ===================================================================
# DERIVED COLUMNS
# Justified by the 5 dashboard questions, the diagnostic document,
# and the proposed PostgreSQL schema.
# ===================================================================
def add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Generate analytically useful columns from confirmed raw fields."""

    # D1: stay_duration_days — from two confirmed datetime columns
    # Dashboard: KPI average LOS (P3), segmentation
    df["stay_duration_days"] = (
        df["Discharge Date"] - df["Date of Admission"]
    ).dt.days
    _log_event("D1_STAY", "stay_duration_days", len(df),
               "Discharge - Admission in days (range 1-30 confirmed)")

    # D2: age_group — binned from Age (confirmed range 13-89)
    # Dashboard: bar charts, cross-filters
    # Diagnosis Issue #9: 116 pediatric patients (13-17) need segmentation
    bins = [0, 17, 30, 45, 60, 75, 100]
    labels = ["13-17", "18-30", "31-45", "46-60", "61-75", "76-89"]
    df["age_group"] = pd.cut(df["Age"], bins=bins, labels=labels, right=True)
    _log_event("D2_AGE_GROUP", "age_group", len(df),
               "6 bands: 13-17 (pediatric) through 76-89")

    # D3: is_pediatric — boolean flag for pediatric segmentation
    # Diagnosis Issue #9: 116 patients aged 13-17
    df["is_pediatric"] = df["Age"] < 18
    pediatric_count = int(df["is_pediatric"].sum())
    _log_event("D3_PEDIATRIC", "is_pediatric", pediatric_count,
               f"Flagged {pediatric_count} patients aged < 18",
               "Diagnosis found 116 pediatric patients needing differentiated analysis")

    # D4: billing_range — quartile-based bands (exclude negatives for boundaries)
    # Dashboard: revenue segmentation (P5)
    positive_billing = df.loc[df["Billing Amount"] >= 0, "Billing Amount"]
    quartile_edges = positive_billing.quantile([0, 0.25, 0.5, 0.75, 1.0]).values
    boundaries = [-np.inf, quartile_edges[1], quartile_edges[2], quartile_edges[3], np.inf]
    df["billing_range"] = pd.cut(
        df["Billing Amount"],
        bins=boundaries,
        labels=["Low", "Medium", "High", "Very High"],
        include_lowest=True,
    )
    _log_event("D4_BILLING_RANGE", "billing_range", len(df),
               "Quartile-based bands (computed on positive amounts only)")

    # D5: admission_month — for time-series in dashboard (P1)
    df["admission_month"] = df["Date of Admission"].dt.to_period("M").astype(str)
    _log_event("D5_ADM_MONTH", "admission_month", len(df), "YYYY-MM for time series")

    # D6: admission_year — for yearly aggregation (P1)
    df["admission_year"] = df["Date of Admission"].dt.year
    _log_event("D6_ADM_YEAR", "admission_year", len(df), "Year extraction")

    # D7: admission_quarter — for quarterly segmentation
    # Aligns with proposed dim_date schema (year, quarter)
    df["admission_quarter"] = df["Date of Admission"].dt.quarter
    _log_event("D7_ADM_QUARTER", "admission_quarter", len(df),
               "Quarter extraction (1-4)")

    # D8: abnormal_test_flag — binary flag for outcome analysis (P4)
    df["abnormal_test_flag"] = (df["Test Results"] == "Abnormal")
    abnormal_count = int(df["abnormal_test_flag"].sum())
    _log_event("D8_ABNORMAL", "abnormal_test_flag", abnormal_count,
               "True where Test Results == Abnormal")

    # D9: is_long_stay — stays above 75th percentile for resource analysis
    p75 = df["stay_duration_days"].quantile(0.75)
    df["is_long_stay"] = df["stay_duration_days"] > p75
    long_count = int(df["is_long_stay"].sum())
    _log_event("D9_LONG_STAY", "is_long_stay", long_count,
               f"stay_duration_days > {p75:.0f} (P75)")

    # D10: negative_outcome_flag — composite risk indicator
    # Combines abnormal test + negative billing for adverse event analysis
    df["negative_outcome_flag"] = (
        df["abnormal_test_flag"] & df["is_billing_negative"]
    )
    neg_outcome_count = int(df["negative_outcome_flag"].sum())
    _log_event("D10_NEG_OUTCOME", "negative_outcome_flag", neg_outcome_count,
               "Abnormal test AND negative billing (compound risk flag)")

    return df


# ===================================================================
# VALIDATION SUITE
# 16 checks covering types, domains, consistency, integrity
# ===================================================================
def run_validations(df: pd.DataFrame) -> list[dict]:
    """Post-transformation validations. Returns a list of check results."""
    checks: list[dict] = []

    def _check(name: str, passed: bool, detail: str = "") -> None:
        status = "PASS" if passed else "FAIL"
        checks.append({"check": name, "status": status, "detail": detail})
        symbol = "OK" if passed else "XX"
        log.info("  [%s] %s: %s", symbol, name, detail)

    # V1: No exact duplicates remain
    dup_count = int(df.duplicated(subset=[
        c for c in df.columns if c not in ("admission_id",)
    ]).sum())
    _check("V01_NO_EXACT_DUPES", dup_count == 0,
           f"{dup_count} exact duplicates remaining")

    # V2: Surrogate key is unique and sequential
    pk_unique = df["admission_id"].is_unique
    pk_sequential = (df["admission_id"].diff().dropna() == 1).all()
    _check("V02_PK_UNIQUE", pk_unique, f"admission_id unique: {pk_unique}")
    _check("V03_PK_SEQUENTIAL", pk_sequential,
           f"admission_id sequential: {pk_sequential}")

    # V3: No NaT in date columns
    nat_total = int(df["Date of Admission"].isna().sum() + df["Discharge Date"].isna().sum())
    _check("V04_DATES_COMPLETE", nat_total == 0,
           f"{nat_total} NaT values in date columns")

    # V4: Discharge >= Admission for all rows
    invalid_dates = int((df["Discharge Date"] < df["Date of Admission"]).sum())
    _check("V05_DATE_COHERENCE", invalid_dates == 0,
           f"{invalid_dates} rows with discharge < admission")

    # V5: stay_duration_days >= 1
    bad_los = int((df["stay_duration_days"] < 1).sum())
    _check("V06_LOS_POSITIVE", bad_los == 0,
           f"{bad_los} rows with stay < 1 day")

    # V6: Age within expected range (13-89 per diagnosis)
    age_bad = int(((df["Age"] < 0) | (df["Age"] > 120)).sum())
    _check("V07_AGE_RANGE", age_bad == 0,
           f"{age_bad} ages outside 0-120 range")

    # V7: Categorical columns have only known categories
    expected = {
        "Gender": {"Male", "Female"},
        "Blood Type": {"A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"},
        "Medical Condition": {"Cancer", "Obesity", "Diabetes", "Asthma",
                              "Hypertension", "Arthritis"},
        "Admission Type": {"Urgent", "Emergency", "Elective"},
        "Medication": {"Paracetamol", "Ibuprofen", "Aspirin", "Penicillin", "Lipitor"},
        "Test Results": {"Normal", "Inconclusive", "Abnormal"},
        "Insurance Provider": {"Blue Cross", "Medicare", "Aetna",
                               "UnitedHealthcare", "Cigna"},
    }
    for col, valid_vals in expected.items():
        actual = set(df[col].dropna().unique())
        extra = actual - valid_vals
        _check(f"V08_DOMAIN_{col}", len(extra) == 0,
               f"Unexpected values: {extra}" if extra else "All values in expected domain")

    # V8: Billing rounded to 2 decimals
    max_decimals = int(df["Billing Amount"].apply(
        lambda x: len(str(x).split(".")[-1]) if "." in str(x) else 0
    ).max())
    _check("V09_BILLING_PRECISION", max_decimals <= 2,
           f"Max decimal places: {max_decimals}")

    # V9: No leading/trailing spaces in string columns
    str_cols = df.select_dtypes(include=["object", "string"]).columns
    space_issues = 0
    for col in str_cols:
        notna = df[col].dropna()
        space_issues += int((notna != notna.str.strip()).sum())
    _check("V10_NO_WHITESPACE", space_issues == 0,
           f"{space_issues} values with leading/trailing spaces")

    # V10: Row count within tolerance
    _check("V11_ROW_COUNT", len(df) >= 54000,
           f"{len(df):,} rows (expected ~54,966 after dedup)")

    # V11: Name column all Title Case
    name_issues = int((df["Name"] != df["Name"].str.title()).sum())
    _check("V12_NAME_TITLE_CASE", name_issues == 0,
           f"{name_issues} names not in Title Case")

    # V12: Hospital no trailing commas
    hosp_comma = int(df["Hospital"].str.endswith(",").sum())
    _check("V13_HOSPITAL_NO_COMMA", hosp_comma == 0,
           f"{hosp_comma} hospitals ending with comma")

    # V13: All derived columns present
    derived_cols = [
        "stay_duration_days", "age_group", "is_pediatric", "billing_range",
        "admission_month", "admission_year", "admission_quarter",
        "abnormal_test_flag", "is_long_stay", "negative_outcome_flag",
    ]
    missing_derived = [c for c in derived_cols if c not in df.columns]
    _check("V14_DERIVED_COLS_PRESENT", len(missing_derived) == 0,
           f"Missing: {missing_derived}" if missing_derived else "All derived columns present")

    # V14: No nulls in critical columns
    critical = ["admission_id", "Name", "Age", "Gender", "Date of Admission",
                "Discharge Date", "Billing Amount", "stay_duration_days"]
    critical_nulls = {c: int(df[c].isna().sum()) for c in critical if df[c].isna().any()}
    _check("V15_CRITICAL_NULLS", len(critical_nulls) == 0,
           f"Nulls in critical columns: {critical_nulls}" if critical_nulls else "No nulls in critical columns")

    # V15: Correct dtypes
    expected_types = {
        "admission_id": "int",
        "Age": "int",
        "Billing Amount": "float",
        "Room Number": "int",
        "stay_duration_days": "int",
        "admission_year": "int",
        "admission_quarter": "int",
    }
    type_issues = []
    for col, expected_type in expected_types.items():
        if col in df.columns and expected_type not in str(df[col].dtype):
            type_issues.append(f"{col}: got {df[col].dtype}, expected {expected_type}")
    _check("V16_DTYPES", len(type_issues) == 0,
           f"Type issues: {type_issues}" if type_issues else "All dtypes correct")

    return checks


# ===================================================================
# REPORTING
# ===================================================================

# --- Transformation rules table ---
TRANSFORMATION_RULES = [
    {
        "rule_id": "R1",
        "problem": "534 exact duplicate rows (0.96%)",
        "transformation": "Drop duplicates keeping first occurrence",
        "columns": "ALL",
        "criteria": "Exact match on all 15 columns; no PK to distinguish legitimate from erroneous",
        "dashboard_impact": "Prevents double-counting in all metrics",
        "residual_risk": "5,500 suspicious near-duplicates remain; same patient+date+doctor but different fields. Kept intentionally — may be legitimate re-admissions.",
    },
    {
        "rule_id": "R2",
        "problem": "No natural primary key",
        "transformation": "Generate sequential surrogate key (admission_id)",
        "columns": "admission_id (new)",
        "criteria": "No column or compound key guarantees uniqueness across 55K rows",
        "dashboard_impact": "Enables reliable row-level joins and drill-down in Metabase",
        "residual_risk": "Key regenerates on re-run; not stable across exports.",
    },
    {
        "rule_id": "R3",
        "problem": "99.94% of patient names with erratic capitalization; 216 with prefixes (Mr./Dr.)",
        "transformation": "Title Case normalization; extract prefix/suffix to separate columns",
        "columns": "Name, name_prefix (new), name_suffix (new)",
        "criteria": "Names like 'BobbY jaCKson' are unreadable in dashboards",
        "dashboard_impact": "Clean display in patient-level views; prefix/suffix available for segmentation",
        "residual_risk": "Names remain high-cardinality (49K+); not useful for grouping.",
    },
    {
        "rule_id": "R4",
        "problem": "2.25% of doctor names not Title Case; 1,124 prefixes, 982 suffixes (MD/PhD/DVM)",
        "transformation": "Title Case + extract professional title to doctor_title column",
        "columns": "Doctor, doctor_title (new)",
        "criteria": "Professional titles inflate cardinality; credential info preserved separately per diagnostic recommendation",
        "dashboard_impact": "Consistent doctor names for top-N and filter dropdowns; credential analysis possible via doctor_title",
        "residual_risk": "Cardinality ~40K. Doctor grouping only useful for top-N queries.",
    },
    {
        "rule_id": "R5",
        "problem": "18,635 hospital names with commas; 5,749 starting with 'and'; 5,602 ending with 'and'",
        "transformation": "Strip trailing commas, leading/trailing 'and', then Title Case",
        "columns": "Hospital",
        "criteria": "Malformed names cause fragmented GROUP BY and dirty Metabase filters",
        "dashboard_impact": "Cleaner hospital filter; reduced unique hospital count",
        "residual_risk": "Cardinality still ~39K; synthetic names cannot be further merged without a master list.",
    },
    {
        "rule_id": "R6",
        "problem": "Billing Amount carries up to 15 decimal digits",
        "transformation": "Round to 2 decimal places",
        "columns": "Billing Amount",
        "criteria": "Currency precision standard — cents are the minimal unit",
        "dashboard_impact": "Clean SUM/AVG calculations; avoids floating-point display artifacts",
        "residual_risk": "None.",
    },
    {
        "rule_id": "R7",
        "problem": "108 records with negative billing (-2,008 to -24)",
        "transformation": "Flag with boolean is_billing_negative; do NOT delete rows",
        "columns": "is_billing_negative (new)",
        "criteria": "Negative amounts may represent adjustments/refunds — no justification column in dataset",
        "dashboard_impact": "Dashboard can filter OUT negatives for revenue metrics, or IN for refund analysis",
        "residual_risk": "If negatives are data errors (not refunds), they pollute billing totals unless filtered.",
    },
    {
        "rule_id": "R8",
        "problem": "40 billing amounts below $100 in absolute value (mean $25,539)",
        "transformation": "Flag with boolean is_billing_low; do NOT delete rows",
        "columns": "is_billing_low (new)",
        "criteria": "Statistical anomalies: amounts $0.50-$99 in a dataset with mean $25K are outliers likely from generation",
        "dashboard_impact": "Analysts can filter or segment these low amounts in billing distributions",
        "residual_risk": "May be generation artifacts; conserved for transparency.",
    },
    {
        "rule_id": "R9",
        "problem": "Date coherence validation (discharge >= admission)",
        "transformation": "Validate and flag; Phase 1 confirmed 0 violations",
        "columns": "Date of Admission, Discharge Date",
        "criteria": "Defensive validation for future data appends",
        "dashboard_impact": "Guarantees LOS and time-series integrity",
        "residual_risk": "None currently. Rule serves as safeguard for incremental loads.",
    },
    {
        "rule_id": "R10",
        "problem": "Whitespace hygiene — defensive cleanup",
        "transformation": "Trim + collapse multi-spaces on all string columns",
        "columns": "ALL STRING",
        "criteria": "Standard pre-load normalization; diagnosis detected 0 issues but best practice for DB loading",
        "dashboard_impact": "Prevents invisible duplicates in Metabase filters",
        "residual_risk": "None.",
    },
]


def generate_before_after(df_raw: pd.DataFrame, df_clean: pd.DataFrame) -> pd.DataFrame:
    """Compare key quality metrics before and after transformation."""
    rows = []

    # Row count
    rows.append({
        "metric": "Total rows",
        "before": len(df_raw),
        "after": len(df_clean),
        "delta": len(df_clean) - len(df_raw),
    })

    # Exact duplicates
    rows.append({
        "metric": "Exact duplicates",
        "before": int(df_raw.duplicated().sum()),
        "after": int(df_clean.duplicated(
            subset=[c for c in df_raw.columns if c in df_clean.columns]
        ).sum()),
        "delta": None,
    })

    # Column count
    rows.append({
        "metric": "Total columns",
        "before": len(df_raw.columns),
        "after": len(df_clean.columns),
        "delta": len(df_clean.columns) - len(df_raw.columns),
    })

    # Name not Title Case
    raw_ntc = int((df_raw["Name"] != df_raw["Name"].str.title()).sum())
    clean_ntc = int((df_clean["Name"] != df_clean["Name"].str.title()).sum())
    rows.append({
        "metric": "Names not Title Case",
        "before": raw_ntc,
        "after": clean_ntc,
        "delta": clean_ntc - raw_ntc,
    })

    # Hospital with trailing commas
    raw_hc = int(df_raw["Hospital"].str.endswith(",").sum())
    clean_hc = int(df_clean["Hospital"].str.endswith(",").sum())
    rows.append({
        "metric": "Hospitals ending with comma",
        "before": raw_hc,
        "after": clean_hc,
        "delta": clean_hc - raw_hc,
    })

    # Negative billing
    rows.append({
        "metric": "Negative billing amounts",
        "before": int((df_raw["Billing Amount"] < 0).sum()),
        "after": int((df_clean["Billing Amount"] < 0).sum()),
        "delta": 0,  # preserved intentionally
    })

    # Low billing
    rows.append({
        "metric": "Low billing (|<$100|)",
        "before": int((df_raw["Billing Amount"].abs() < 100).sum()),
        "after": int((df_clean["Billing Amount"].abs() < 100).sum()),
        "delta": 0,  # preserved intentionally
    })

    # Max billing decimals
    def _max_dec(s: pd.Series) -> int:
        return int(s.apply(
            lambda x: len(str(x).split(".")[-1]) if "." in str(x) else 0
        ).max())
    rows.append({
        "metric": "Max billing decimal places",
        "before": _max_dec(df_raw["Billing Amount"]),
        "after": _max_dec(df_clean["Billing Amount"]),
        "delta": None,
    })

    # Doctor titles mixed in name
    raw_dt = int(df_raw["Doctor"].str.contains(
        r"\s+(?:MD|DVM|PhD)\s*$", flags=re.IGNORECASE, regex=True, na=False
    ).sum())
    clean_dt = int(df_clean["Doctor"].str.contains(
        r"\s+(?:MD|DVM|PhD)\s*$", flags=re.IGNORECASE, regex=True, na=False
    ).sum())
    rows.append({
        "metric": "Doctors with embedded professional titles",
        "before": raw_dt,
        "after": clean_dt,
        "delta": clean_dt - raw_dt,
    })

    return pd.DataFrame(rows)


def generate_invalid_records(df: pd.DataFrame) -> pd.DataFrame:
    """Collect rows with any quality flag for audit."""
    mask = pd.Series(False, index=df.index)

    if "is_billing_negative" in df.columns:
        mask |= df["is_billing_negative"]
    if "is_billing_low" in df.columns:
        mask |= df["is_billing_low"]
    if "date_quality_flag" in df.columns:
        mask |= df["date_quality_flag"].notna()

    invalid = df.loc[mask].copy()

    # Build flag_reason column
    reasons = pd.Series("", index=invalid.index, dtype=str)
    if "is_billing_negative" in df.columns:
        reasons = reasons.where(
            ~invalid["is_billing_negative"], reasons + "negative_billing;"
        )
    if "is_billing_low" in df.columns:
        reasons = reasons.where(
            ~invalid["is_billing_low"], reasons + "low_billing;"
        )
    if "date_quality_flag" in df.columns:
        date_flagged = invalid["date_quality_flag"].notna()
        reasons = reasons.where(~date_flagged, reasons + "date_incoherence;")
    invalid["flag_reason"] = reasons

    return invalid


def generate_derived_dictionary() -> pd.DataFrame:
    """Document every derived column with its source, logic, and purpose."""
    rows = [
        {
            "column": "admission_id",
            "source_columns": "None (generated)",
            "dtype": "int64",
            "logic": "Sequential integer 1..N assigned after deduplication",
            "dashboard_use": "Primary key for joins and drill-down",
        },
        {
            "column": "stay_duration_days",
            "source_columns": "Discharge Date, Date of Admission",
            "dtype": "int64",
            "logic": "(Discharge Date - Date of Admission).days",
            "dashboard_use": "KPI average LOS; histogram; segmentation by medical condition",
        },
        {
            "column": "age_group",
            "source_columns": "Age",
            "dtype": "category",
            "logic": "Bins: 13-17 (pediatric), 18-30, 31-45, 46-60, 61-75, 76-89",
            "dashboard_use": "Age-band bar charts and cross-filters",
        },
        {
            "column": "is_pediatric",
            "source_columns": "Age",
            "dtype": "bool",
            "logic": "True where Age < 18 (116 patients per diagnosis)",
            "dashboard_use": "Pediatric segment filter; differentiated analysis per diagnostic recommendation",
        },
        {
            "column": "billing_range",
            "source_columns": "Billing Amount",
            "dtype": "category",
            "logic": "Quartile-based on positive amounts: Low, Medium, High, Very High",
            "dashboard_use": "Revenue tier segmentation; cost distribution analysis (P5)",
        },
        {
            "column": "admission_month",
            "source_columns": "Date of Admission",
            "dtype": "str (YYYY-MM)",
            "logic": "Period extraction to_period('M')",
            "dashboard_use": "Monthly time-series line charts (P1)",
        },
        {
            "column": "admission_year",
            "source_columns": "Date of Admission",
            "dtype": "int64",
            "logic": "Year extraction from Date of Admission",
            "dashboard_use": "Year-over-year comparisons; yearly filters",
        },
        {
            "column": "admission_quarter",
            "source_columns": "Date of Admission",
            "dtype": "int64",
            "logic": "Quarter extraction (1-4) from Date of Admission",
            "dashboard_use": "Quarterly aggregation; aligns with proposed dim_date schema",
        },
        {
            "column": "abnormal_test_flag",
            "source_columns": "Test Results",
            "dtype": "bool",
            "logic": "True where Test Results == 'Abnormal'",
            "dashboard_use": "Abnormal rate KPI (P4); outcome analysis filters",
        },
        {
            "column": "is_long_stay",
            "source_columns": "stay_duration_days",
            "dtype": "bool",
            "logic": "True where stay_duration_days > P75 (75th percentile)",
            "dashboard_use": "Resource utilization analysis; long-stay rate KPI",
        },
        {
            "column": "negative_outcome_flag",
            "source_columns": "abnormal_test_flag, is_billing_negative",
            "dtype": "bool",
            "logic": "True where BOTH abnormal test AND negative billing (compound risk)",
            "dashboard_use": "Adverse event indicator; compound risk segmentation",
        },
        {
            "column": "is_billing_negative",
            "source_columns": "Billing Amount",
            "dtype": "bool",
            "logic": "True where Billing Amount < 0 (108 rows per diagnosis)",
            "dashboard_use": "Filter toggle: exclude from revenue or isolate refunds/adjustments",
        },
        {
            "column": "is_billing_low",
            "source_columns": "Billing Amount",
            "dtype": "bool",
            "logic": "True where |Billing Amount| < $100 (40 rows per diagnosis)",
            "dashboard_use": "Anomaly filter; billing distribution outlier analysis",
        },
        {
            "column": "doctor_title",
            "source_columns": "Doctor",
            "dtype": "str (nullable)",
            "logic": "Extracted professional suffix (MD, PhD, DVM, Jr., Sr.) before cleaning",
            "dashboard_use": "Credential analysis; doctor segmentation by qualification",
        },
        {
            "column": "name_prefix",
            "source_columns": "Name",
            "dtype": "str (nullable)",
            "logic": "Extracted prefix (Mr., Dr., Mrs., etc.) before cleaning",
            "dashboard_use": "Optional demographic enrichment",
        },
        {
            "column": "name_suffix",
            "source_columns": "Name",
            "dtype": "str (nullable)",
            "logic": "Extracted suffix (Jr., II, III) before cleaning",
            "dashboard_use": "Optional demographic enrichment",
        },
    ]
    return pd.DataFrame(rows)


def write_transformation_report(
    checks: list[dict],
    before_after: pd.DataFrame,
    out_dir: Path,
) -> None:
    """Write the full Markdown transformation report."""
    lines = [
        "# Transformation Report — Phase 2",
        "",
        f"**Generated:** {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}",
        f"**Source:** `data/raw/healthcare_dataset.csv`",
        f"**Output:** `data/processed/healthcare_clean.csv`",
        f"**Diagnostic basis:** `reports/diagnosis_report.json`, `docs/01_diagnostico_tecnico.md`",
        "",
        "---",
        "",
        "## 1. Transformation Rules Applied",
        "",
        "Each rule maps to a confirmed finding from Phase 1 diagnostics.",
        "",
    ]

    for r in TRANSFORMATION_RULES:
        lines += [
            f"### {r['rule_id']} — {r['problem'][:90]}",
            "",
            f"- **Transformation:** {r['transformation']}",
            f"- **Columns affected:** {r['columns']}",
            f"- **Technical criteria:** {r['criteria']}",
            f"- **Dashboard impact:** {r['dashboard_impact']}",
            f"- **Residual risk:** {r['residual_risk']}",
            "",
        ]

    lines += [
        "---",
        "",
        "## 2. Before / After Comparison",
        "",
        before_after.to_markdown(index=False),
        "",
        "---",
        "",
        "## 3. Transformation Log",
        "",
        "See `reports/transformation_log.csv` for the full event-level log with traceability.",
        "",
        f"**Total events logged:** {len(_transform_log)}",
        "",
        "---",
        "",
        "## 4. Post-Transformation Validations",
        "",
        "| Check | Status | Detail |",
        "|-------|--------|--------|",
    ]
    for c in checks:
        lines.append(f"| {c['check']} | {c['status']} | {c['detail']} |")

    passed = sum(1 for c in checks if c["status"] == "PASS")
    total = len(checks)
    lines += [
        "",
        f"**Result: {passed}/{total} checks passed.**",
        "",
        "---",
        "",
        "## 5. Derived Columns",
        "",
        "See `reports/derived_columns_dictionary.csv` for full documentation of all",
        f"derived columns added during transformation.",
        "",
        "---",
        "",
        "## 6. Invalid/Flagged Records",
        "",
        "See `reports/invalid_records.csv` for all rows with quality flags.",
        "Each row includes a `flag_reason` column indicating why it was flagged.",
        "",
        "---",
        "",
        "## 7. Readiness Criteria for Phase 3",
        "",
        "The dataset is ready for SQL schema modeling and PostgreSQL loading when:",
        "",
        "1. All validation checks above show **PASS**",
        "2. `healthcare_clean.csv` has the expected ~54,966 rows",
        "3. All derived columns are populated without nulls in critical fields",
        "4. The data dictionary matches the target PostgreSQL schema",
        "5. No unresolved FAIL validations exist",
        "6. `transformation_log.csv` documents every change applied",
        "",
        "---",
        "",
        "## 8. Ambiguities and Decisions Log",
        "",
        "| Item | Decision | Justification |",
        "|------|----------|---------------|",
        "| 108 negative billing amounts | **Preserved + flagged** | No column explains if refund/adjustment/error; deleting loses audit trail |",
        "| 40 low billing amounts (<$100) | **Preserved + flagged** | Statistical outliers vs mean $25K; may be generation artifacts |",
        "| 5,500 near-duplicate rows | **Preserved** | Different field values suggest legitimate re-admissions; cannot confirm without PK |",
        "| Professional titles in Doctor | **Extracted to doctor_title** | Preserves credential info while cleaning name for GROUP BY |",
        "| Uniform distributions | **Documented, no action** | Confirmed synthetic data characteristic; no correction possible |",
    ]

    (out_dir / "transformation_report.md").write_text(
        "\n".join(lines), encoding="utf-8"
    )


# ===================================================================
# MAIN PIPELINE
# ===================================================================
def main() -> None:
    log.info("=" * 60)
    log.info("Phase 2 — Healthcare Dataset Transformation")
    log.info("=" * 60)

    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    # Load raw (keep a copy for before/after)
    log.info("Loading raw data...")
    df_raw = load_raw(DATA_RAW)
    log.info("Raw: %s rows x %s columns", f"{df_raw.shape[0]:,}", df_raw.shape[1])

    df = df_raw.copy()

    # Apply transformation rules in order
    log.info("--- Applying transformation rules ---")
    df = remove_exact_duplicates(df)    # R1
    df = add_surrogate_key(df)          # R2
    df = normalize_names(df)            # R3
    df = normalize_doctors(df)          # R4
    df = clean_hospital_names(df)       # R5
    df = round_billing(df)              # R6
    df = flag_negative_billing(df)      # R7
    df = flag_low_billing(df)           # R8
    df = validate_dates(df)             # R9
    df = clean_whitespace(df)           # R10

    # Derived columns
    log.info("--- Adding derived columns ---")
    df = add_derived_columns(df)

    # Validations
    log.info("--- Running post-transformation validations ---")
    checks = run_validations(df)

    # Exports
    log.info("--- Writing outputs ---")

    # Clean CSV
    clean_path = DATA_PROCESSED / "healthcare_clean.csv"
    df.to_csv(clean_path, index=False)
    log.info("-> %s (%s rows x %s cols)", clean_path.relative_to(PROJECT_ROOT),
             f"{len(df):,}", len(df.columns))

    # Transformation log
    log_df = pd.DataFrame(_transform_log)
    log_path = REPORTS_DIR / "transformation_log.csv"
    log_df.to_csv(log_path, index=False)
    log.info("-> %s (%s entries)", log_path.relative_to(PROJECT_ROOT), len(log_df))

    # Before/after comparison
    ba_df = generate_before_after(df_raw, df)
    ba_path = REPORTS_DIR / "data_quality_before_after.csv"
    ba_df.to_csv(ba_path, index=False)
    log.info("-> %s", ba_path.relative_to(PROJECT_ROOT))

    # Invalid records
    invalid_df = generate_invalid_records(df)
    inv_path = REPORTS_DIR / "invalid_records.csv"
    invalid_df.to_csv(inv_path, index=False)
    log.info("-> %s (%s flagged rows)", inv_path.relative_to(PROJECT_ROOT), len(invalid_df))

    # Derived columns dictionary
    dict_df = generate_derived_dictionary()
    dict_path = REPORTS_DIR / "derived_columns_dictionary.csv"
    dict_df.to_csv(dict_path, index=False)
    log.info("-> %s", dict_path.relative_to(PROJECT_ROOT))

    # Transformation report
    write_transformation_report(checks, ba_df, REPORTS_DIR)
    log.info("-> %s", (REPORTS_DIR / "transformation_report.md").relative_to(PROJECT_ROOT))

    # Final summary
    passed = sum(1 for c in checks if c["status"] == "PASS")
    total = len(checks)
    log.info("=" * 60)
    log.info("Phase 2 complete. %s/%s validations passed.", passed, total)
    log.info("Clean dataset: %s rows x %s columns", f"{len(df):,}", len(df.columns))
    log.info("=" * 60)

    if passed < total:
        failed = [c for c in checks if c["status"] == "FAIL"]
        log.warning("FAILED validations:")
        for f in failed:
            log.warning("  %s: %s", f["check"], f["detail"])
        sys.exit(1)
    else:
        log.info("Dataset is READY for Phase 3 (schema modeling and DB load).")


if __name__ == "__main__":
    main()
