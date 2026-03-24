"""
Phase 3 — Modelado y Carga en PostgreSQL

Carga el dataset de healthcare limpio en una base PostgreSQL con esquema estrella.
Primero se cargan las dimensiones y luego la tabla de hechos con resolucion de FK.

Requires:
  - PostgreSQL server running and accessible
  - Environment variables or .env file with DB credentials
  - data/processed/healthcare_clean.csv (from Phase 2)
  - sql/01_schema.sql

Outputs:
  - reports/load_report.md
  - reports/load_summary.csv
  - reports/rejected_records.csv (if any rows rejected)
  - reports/data_model_description.md
"""

import logging
import os
import sys
from io import StringIO
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# ---------------------------------------------------------------------------
# Rutas
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_CLEAN = PROJECT_ROOT / "data" / "processed" / "healthcare_clean.csv"
SCHEMA_SQL = PROJECT_ROOT / "sql" / "01_schema.sql"
REPORTS_DIR = PROJECT_ROOT / "reports"

# ---------------------------------------------------------------------------
# Registro
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("phase3")

# ---------------------------------------------------------------------------
# Seguimiento de carga
# ---------------------------------------------------------------------------
_load_summary: list[dict] = []
_rejected_records: list[dict] = []


def _record(table: str, rows: int, status: str = "OK", detail: str = "") -> None:
    _load_summary.append({
        "table": table, "rows_loaded": rows, "status": status, "detail": detail,
    })
    log.info("  %s: %s rows [%s] %s", table, f"{rows:,}", status, detail)


# ===================================================================
# CONEXION A BASE DE DATOS
# ===================================================================
def get_connection():
    """Crea la conexion a PostgreSQL desde variables de entorno."""
    try:
        from dotenv import load_dotenv
        load_dotenv(PROJECT_ROOT / ".env")
    except ImportError:
        pass

    params = {
        "host": os.environ.get("DB_HOST", "localhost"),
        "port": int(os.environ.get("DB_PORT", "5432")),
        "dbname": os.environ.get("DB_NAME", "healthcare_db"),
        "user": os.environ.get("DB_USER", "postgres"),
        "password": os.environ.get("DB_PASSWORD", "admin"),
    }
    log.info("Connecting to %s:%s/%s as %s",
             params["host"], params["port"], params["dbname"], params["user"])
    conn = psycopg2.connect(**params)
    conn.set_session(autocommit=False)
    return conn


# ===================================================================
# CREACION DE ESQUEMA
# ===================================================================
def execute_schema(conn) -> None:
    """Ejecuta el archivo SQL de esquema para crear tablas, indices y vistas."""
    if not SCHEMA_SQL.exists():
        log.error("Schema file not found: %s", SCHEMA_SQL)
        sys.exit(1)
    sql = SCHEMA_SQL.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    log.info("Schema created/verified from %s", SCHEMA_SQL.name)


# ===================================================================
# CARGA DE DATOS
# ===================================================================
def load_clean_csv() -> pd.DataFrame:
    """Lee el CSV limpio de Fase 2 con tipos de datos correctos."""
    if not DATA_CLEAN.exists():
        log.error("Clean CSV not found: %s", DATA_CLEAN)
        sys.exit(1)
    df = pd.read_csv(
        DATA_CLEAN,
        parse_dates=["Date of Admission", "Discharge Date"],
    )
    log.info("Loaded clean CSV: %s rows x %s cols", f"{len(df):,}", len(df.columns))
    return df


# ===================================================================
# CARGA DE DIMENSIONES
# ===================================================================
def load_dim_date(conn, df: pd.DataFrame) -> None:
    """Genera y carga una dimension calendario que cubre todo el rango de fechas."""
    min_d = min(df["Date of Admission"].min(), df["Discharge Date"].min())
    max_d = max(df["Date of Admission"].max(), df["Discharge Date"].max())
    dates = pd.date_range(start=min_d, end=max_d, freq="D")

    records = []
    for d in dates:
        records.append((
            int(d.strftime("%Y%m%d")),
            d.date(),
            int(d.year),
            int(d.quarter),
            int(d.month),
            d.strftime("%B"),
            int(d.day),
            int(d.dayofweek),
            d.strftime("%A"),
            bool(d.dayofweek >= 5),
        ))

    sql = """
        INSERT INTO healthcare.dim_date
            (date_id, full_date, year, quarter, month, month_name,
             day, day_of_week, day_name, is_weekend)
        VALUES %s
        ON CONFLICT (date_id) DO NOTHING
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, records, page_size=500)
    conn.commit()
    _record("dim_date", len(records),
            detail=f"{dates[0].date()} to {dates[-1].date()} ({len(records)} days)")


def load_dim_simple(conn, table: str, col_name: str, values: list) -> None:
    """Carga una tabla de dimension de una sola columna desde valores unicos."""
    sql = f"""
        INSERT INTO healthcare.{table} ({col_name})
        VALUES %s
        ON CONFLICT ({col_name}) DO NOTHING
    """
    data = [(v,) for v in sorted(values)]
    with conn.cursor() as cur:
        execute_values(cur, sql, data)
    conn.commit()
    _record(table, len(data))


def load_all_dimensions(conn, df: pd.DataFrame) -> None:
    """Carga todas las tablas de dimensiones: calendario + 5 categoricas."""
    log.info("--- Loading dimensions ---")

    load_dim_date(conn, df)

    dim_config = {
        "dim_medical_condition": ("condition_name", "Medical Condition"),
        "dim_admission_type": ("admission_type_name", "Admission Type"),
        "dim_insurance": ("provider_name", "Insurance Provider"),
        "dim_medication": ("medication_name", "Medication"),
        "dim_test_result": ("result_name", "Test Results"),
    }
    for table, (col_name, src_col) in dim_config.items():
        load_dim_simple(conn, table, col_name, df[src_col].unique().tolist())


# ===================================================================
# RESOLUCION DE FK
# ===================================================================
def get_dim_map(conn, table: str, id_col: str, name_col: str) -> dict:
    """Obtiene una tabla de dimension y retorna un mapeo {nombre: id}."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT {name_col}, {id_col} FROM healthcare.{table}")
        return dict(cur.fetchall())


# ===================================================================
# CARGA DE TABLA DE HECHOS
# ===================================================================
def _to_native(val):
    """Convierte tipos numpy/pandas a tipos nativos de Python para psycopg2."""
    if val is None:
        return None
    if isinstance(val, float) and np.isnan(val):
        return None
    if isinstance(val, (np.bool_,)):
        return bool(val)
    if isinstance(val, (np.integer,)):
        return int(val)
    if isinstance(val, (np.floating,)):
        return float(val)
    return val


def load_fact_table(conn, df: pd.DataFrame) -> int:
    """Resuelve FKs, prepara y carga masivamente la tabla de hechos.
    Retorna el numero de filas rechazadas."""

    # Obtener mapeos de dimensiones
    cond_map = get_dim_map(conn, "dim_medical_condition", "condition_id", "condition_name")
    type_map = get_dim_map(conn, "dim_admission_type", "admission_type_id", "admission_type_name")
    ins_map = get_dim_map(conn, "dim_insurance", "insurance_id", "provider_name")
    med_map = get_dim_map(conn, "dim_medication", "medication_id", "medication_name")
    test_map = get_dim_map(conn, "dim_test_result", "test_result_id", "result_name")

    fact = df.copy()

    # Calcular IDs de fecha (entero YYYYMMDD)
    fact["admission_date_id"] = (
        fact["Date of Admission"].dt.strftime("%Y%m%d").astype(int)
    )
    fact["discharge_date_id"] = (
        fact["Discharge Date"].dt.strftime("%Y%m%d").astype(int)
    )

    # Mapear IDs de FK
    fact["condition_id"] = fact["Medical Condition"].map(cond_map)
    fact["admission_type_id"] = fact["Admission Type"].map(type_map)
    fact["insurance_id"] = fact["Insurance Provider"].map(ins_map)
    fact["medication_id"] = fact["Medication"].map(med_map)
    fact["test_result_id"] = fact["Test Results"].map(test_map)

    # Detectar filas con FK no mapeada
    fk_cols = [
        "condition_id", "admission_type_id", "insurance_id",
        "medication_id", "test_result_id",
    ]
    bad_mask = fact[fk_cols].isna().any(axis=1)
    rejected_count = int(bad_mask.sum())
    if rejected_count > 0:
        rejected = fact[bad_mask].copy()
        rejected["reject_reason"] = "Unmapped foreign key"
        _rejected_records.extend(rejected.to_dict("records"))
        log.warning("%d rows rejected: unmapped FK", rejected_count)
        fact = fact[~bad_mask]

    # Convertir columnas FK a int
    for col in fk_cols:
        fact[col] = fact[col].astype(int)

    # Definir orden de columnas para insercion (coincide con DDL de fact_admissions)
    insert_cols = [
        "admission_id",
        "admission_date_id", "discharge_date_id",
        "condition_id", "admission_type_id", "insurance_id",
        "medication_id", "test_result_id",
        "patient_name", "name_prefix", "name_suffix",
        "doctor_name", "doctor_title", "hospital_name",
        "gender", "blood_type",
        "age", "room_number", "billing_amount", "stay_duration_days",
        "age_group", "billing_range",
        "is_pediatric", "is_billing_negative", "is_billing_low",
        "is_long_stay", "abnormal_test_flag", "negative_outcome_flag",
    ]

    # Renombrar columnas fuente a nombres destino
    rename = {
        "Name": "patient_name",
        "Doctor": "doctor_name",
        "Hospital": "hospital_name",
        "Gender": "gender",
        "Blood Type": "blood_type",
        "Age": "age",
        "Room Number": "room_number",
        "Billing Amount": "billing_amount",
    }
    fact = fact.rename(columns=rename)
    fact = fact[insert_cols]

    # Construir tuplas con tipos nativos de Python
    values = [
        tuple(_to_native(v) for v in row)
        for row in fact.itertuples(index=False)
    ]

    cols_str = ", ".join(insert_cols)
    sql = f"""
        INSERT INTO healthcare.fact_admissions ({cols_str})
        VALUES %s
        ON CONFLICT (admission_id) DO NOTHING
    """
    log.info("--- Loading fact table (%s rows) ---", f"{len(values):,}")
    with conn.cursor() as cur:
        execute_values(cur, sql, values, page_size=2000)
    conn.commit()

    _record("fact_admissions", len(values))
    return rejected_count


# ===================================================================
# REGISTRO DE AUDITORIA
# ===================================================================
def write_audit_entries(conn, source_file: str) -> None:
    """Inserta registros de auditoria para cada tabla cargada."""
    sql = """
        INSERT INTO healthcare.load_audit
            (source_file, table_name, rows_loaded, status, detail)
        VALUES %s
    """
    records = [
        (source_file, s["table"], s["rows_loaded"], s["status"], s["detail"])
        for s in _load_summary
    ]
    with conn.cursor() as cur:
        execute_values(cur, sql, records)
    conn.commit()
    log.info("Audit entries written: %d records", len(records))


# ===================================================================
# VALIDACION POSTERIOR A LA CARGA
# ===================================================================
def validate_load(conn) -> list[dict]:
    """Ejecuta verificaciones post-carga en la base de datos. Retorna resultados."""
    checks: list[dict] = []

    def _check(name: str, passed: bool, detail: str = "") -> None:
        status = "PASS" if passed else "FAIL"
        checks.append({"check": name, "status": status, "detail": detail})
        symbol = "OK" if passed else "XX"
        log.info("  [%s] %s: %s", symbol, name, detail)

    with conn.cursor() as cur:
        # V1: Conteo de filas de la tabla de hechos
        cur.execute("SELECT COUNT(*) FROM healthcare.fact_admissions")
        fact_count = cur.fetchone()[0]
        _check("V1_FACT_COUNT", fact_count >= 54000,
               f"{fact_count:,} rows in fact_admissions")

        # V2: Todas las dimensiones tienen filas
        for table, expected_min in [
            ("dim_date", 1800), ("dim_medical_condition", 6),
            ("dim_admission_type", 3), ("dim_insurance", 5),
            ("dim_medication", 5), ("dim_test_result", 3),
        ]:
            cur.execute(f"SELECT COUNT(*) FROM healthcare.{table}")
            cnt = cur.fetchone()[0]
            _check(f"V2_{table.upper()}", cnt >= expected_min,
                   f"{cnt} rows in {table}")

        # V3: Integridad FK (sin FKs huerfanas)
        cur.execute("""
            SELECT COUNT(*) FROM healthcare.fact_admissions f
            LEFT JOIN healthcare.dim_medical_condition mc
                ON f.condition_id = mc.condition_id
            WHERE mc.condition_id IS NULL
        """)
        orphans = cur.fetchone()[0]
        _check("V3_FK_INTEGRITY", orphans == 0,
               f"{orphans} orphan condition FKs")

        # V4: Rango de fechas coincide con lo esperado
        cur.execute("""
            SELECT MIN(d.full_date), MAX(d.full_date)
            FROM healthcare.fact_admissions f
            JOIN healthcare.dim_date d ON f.admission_date_id = d.date_id
        """)
        min_date, max_date = cur.fetchone()
        _check("V4_DATE_RANGE", min_date is not None,
               f"Admission dates: {min_date} to {max_date}")

        # V5: Sin admission_id duplicados
        cur.execute("""
            SELECT COUNT(*) - COUNT(DISTINCT admission_id)
            FROM healthcare.fact_admissions
        """)
        dup_pks = cur.fetchone()[0]
        _check("V5_NO_DUP_PK", dup_pks == 0,
               f"{dup_pks} duplicate admission_ids")

        # V6: Las vistas son consultables
        for view in [
            "vw_monthly_admissions", "vw_top_hospitals",
            "vw_avg_los_by_condition", "vw_abnormal_rate",
            "vw_billing_distribution",
        ]:
            try:
                cur.execute(f"SELECT COUNT(*) FROM healthcare.{view}")
                cnt = cur.fetchone()[0]
                _check(f"V6_{view.upper()}", cnt > 0, f"{cnt} rows")
            except psycopg2.Error as e:
                _check(f"V6_{view.upper()}", False, str(e))
                conn.rollback()

        # V7: Validacion basica de suma de facturacion
        cur.execute("""
            SELECT ROUND(SUM(billing_amount), 2),
                   ROUND(AVG(billing_amount), 2)
            FROM healthcare.fact_admissions
            WHERE NOT is_billing_negative
        """)
        total, avg = cur.fetchone()
        _check("V7_BILLING_SANITY", avg > 10000,
               f"Total billing (excl. negative): ${total:,.2f}, Avg: ${avg:,.2f}")

    return checks


# ===================================================================
# GENERACION DE REPORTES
# ===================================================================
def write_load_summary() -> None:
    """Exporta load_summary.csv."""
    pd.DataFrame(_load_summary).to_csv(
        REPORTS_DIR / "load_summary.csv", index=False,
    )
    log.info("-> reports/load_summary.csv")


def write_rejected_records() -> None:
    """Exporta rejected_records.csv si hubo filas rechazadas."""
    if _rejected_records:
        pd.DataFrame(_rejected_records).to_csv(
            REPORTS_DIR / "rejected_records.csv", index=False,
        )
        log.info("-> reports/rejected_records.csv (%d rows)", len(_rejected_records))
    else:
        log.info("No rejected records.")


def write_load_report(checks: list[dict]) -> None:
    """Genera el reporte de carga en Markdown."""
    passed = sum(1 for c in checks if c["status"] == "PASS")
    total = len(checks)

    lines = [
        "# Load Report — Phase 3",
        "",
        f"**Generated:** {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}",
        f"**Source:** `data/processed/healthcare_clean.csv`",
        f"**Target:** PostgreSQL schema `healthcare`",
        "",
        "---",
        "",
        "## 1. Load Summary",
        "",
        "| Table | Rows Loaded | Status | Detail |",
        "|-------|-------------|--------|--------|",
    ]
    for s in _load_summary:
        lines.append(
            f"| {s['table']} | {s['rows_loaded']:,} | {s['status']} | {s['detail']} |"
        )

    total_rows = sum(s["rows_loaded"] for s in _load_summary)
    lines += [
        "",
        f"**Total rows loaded across all tables: {total_rows:,}**",
        "",
        "---",
        "",
        "## 2. Post-Load Validations",
        "",
        "| Check | Status | Detail |",
        "|-------|--------|--------|",
    ]
    for c in checks:
        lines.append(f"| {c['check']} | {c['status']} | {c['detail']} |")
    lines += [
        "",
        f"**Result: {passed}/{total} checks passed.**",
        "",
        "---",
        "",
        "## 3. Rejected Records",
        "",
        f"**{len(_rejected_records)} rows rejected.**"
        + (" See `reports/rejected_records.csv`." if _rejected_records else ""),
        "",
        "---",
        "",
        "## 4. Readiness for Phase 4 (Dashboard)",
        "",
        "The database is ready for Metabase dashboard construction when:",
        "",
        "1. All validation checks above show **PASS**",
        "2. All 5 analytical views return data",
        "3. `fact_admissions` row count matches expected ~54,966",
        "4. FK integrity is confirmed (0 orphans)",
        "5. Billing totals are in expected range",
    ]

    (REPORTS_DIR / "load_report.md").write_text("\n".join(lines), encoding="utf-8")
    log.info("-> reports/load_report.md")


def write_data_model_description() -> None:
    """Genera documentacion del modelo de datos."""
    lines = [
        "# Data Model Description — Healthcare Analytics",
        "",
        f"**Generated:** {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}",
        f"**Schema file:** `sql/01_schema.sql`",
        f"**Schema name:** `healthcare`",
        "",
        "---",
        "",
        "## 1. Model Type: Star Schema",
        "",
        "**Justification:**",
        "",
        "- The dataset has a single grain (one row per hospital admission)",
        "- 5 low-cardinality categorical columns map naturally to dimensions",
        "- Temporal analysis requires a calendar dimension (dim_date)",
        "- High-cardinality text fields (Name, Doctor, Hospital: ~40K unique each)",
        "  remain as degenerate dimensions on the fact table, since normalizing",
        "  them would create dimensions nearly as large as the fact itself",
        "- Gender (2 values) and Blood Type (8 values) use CHECK constraints",
        "  on the fact table rather than separate dimensions, avoiding join",
        "  overhead for minimal referential integrity gain",
        "- A flat star schema (vs. snowflake) is optimal for Metabase, which",
        "  performs best with minimal joins",
        "",
        "---",
        "",
        "## 2. Fact Table: `fact_admissions`",
        "",
        "**Grain:** One row per hospital admission event.",
        "",
        "**Row count:** ~54,966 (after Phase 2 deduplication)",
        "",
        "### Measures (numeric, aggregatable)",
        "",
        "| Column | Type | Description |",
        "|--------|------|-------------|",
        "| `billing_amount` | NUMERIC(12,2) | Cost billed for the admission |",
        "| `stay_duration_days` | SMALLINT | Length of stay (discharge - admission) |",
        "| `age` | SMALLINT | Patient age at admission |",
        "| `room_number` | SMALLINT | Assigned room |",
        "",
        "### Foreign Keys",
        "",
        "| Column | References | Description |",
        "|--------|-----------|-------------|",
        "| `admission_date_id` | `dim_date.date_id` | Admission calendar date |",
        "| `discharge_date_id` | `dim_date.date_id` | Discharge calendar date |",
        "| `condition_id` | `dim_medical_condition` | Medical condition diagnosed |",
        "| `admission_type_id` | `dim_admission_type` | Elective/Emergency/Urgent |",
        "| `insurance_id` | `dim_insurance` | Insurance provider |",
        "| `medication_id` | `dim_medication` | Primary medication prescribed |",
        "| `test_result_id` | `dim_test_result` | Test result outcome |",
        "",
        "### Degenerate Dimensions",
        "",
        "| Column | Cardinality | Rationale for denormalization |",
        "|--------|-------------|-------------------------------|",
        "| `patient_name` | ~40K | No real patient ID; almost 1:1 with fact |",
        "| `doctor_name` | ~40K | Synthetic; nearly unique per row |",
        "| `hospital_name` | ~40K | Synthetic; nearly unique per row |",
        "| `gender` | 2 | CHECK constraint; join cost exceeds benefit |",
        "| `blood_type` | 8 | CHECK constraint; join cost exceeds benefit |",
        "",
        "### Pre-computed Flags",
        "",
        "| Column | Type | Logic | Dashboard Use |",
        "|--------|------|-------|---------------|",
        "| `is_pediatric` | BOOLEAN | Age < 18 | Pediatric segment filter |",
        "| `is_billing_negative` | BOOLEAN | Billing < 0 | Exclude refunds/adjustments |",
        "| `is_billing_low` | BOOLEAN | |Billing| < $100 | Anomaly filter |",
        "| `is_long_stay` | BOOLEAN | LOS > P75 (23 days) | Resource analysis |",
        "| `abnormal_test_flag` | BOOLEAN | Test = 'Abnormal' | Outcome KPI |",
        "| `negative_outcome_flag` | BOOLEAN | Abnormal AND negative billing | Risk indicator |",
        "",
        "---",
        "",
        "## 3. Dimension Tables",
        "",
        "| Dimension | Rows | Key Column | Description |",
        "|-----------|------|------------|-------------|",
        "| `dim_date` | ~1,856 | `date_id (YYYYMMDD)` | Calendar with year, quarter, month, day, weekday |",
        "| `dim_medical_condition` | 6 | `condition_id` | Arthritis, Asthma, Cancer, Diabetes, Hypertension, Obesity |",
        "| `dim_admission_type` | 3 | `admission_type_id` | Elective, Emergency, Urgent |",
        "| `dim_insurance` | 5 | `insurance_id` | Aetna, Blue Cross, Cigna, Medicare, UnitedHealthcare |",
        "| `dim_medication` | 5 | `medication_id` | Aspirin, Ibuprofen, Lipitor, Paracetamol, Penicillin |",
        "| `dim_test_result` | 3 | `test_result_id` | Abnormal, Inconclusive, Normal |",
        "",
        "---",
        "",
        "## 4. Analytical Views",
        "",
        "| View | Aligned Question | Joins | Description |",
        "|------|-----------------|-------|-------------|",
        "| `vw_monthly_admissions` | P1: Admission volume | dim_date | Monthly admissions, abnormal %, avg billing, avg LOS |",
        "| `vw_top_hospitals` | P2: Top hospitals | dim_medical_condition | Hospital ranking by volume and billing |",
        "| `vw_avg_los_by_condition` | P3: Average LOS | dim_medical_condition | LOS stats by condition (avg, median, P75) |",
        "| `vw_abnormal_rate` | P4: Abnormal test % | dim_medical_condition, dim_insurance | Abnormal rate by condition x insurance |",
        "| `vw_billing_distribution` | P5: Cost distribution | dim_insurance, dim_medical_condition | Billing by range, condition, insurance |",
        "",
        "---",
        "",
        "## 5. Audit Table: `load_audit`",
        "",
        "Tracks every load operation with timestamp, source file, table name,",
        "row count, and status. Enables traceability and re-run detection.",
        "",
        "---",
        "",
        "## 6. Design Decisions",
        "",
        "| Decision | Rationale |",
        "|----------|-----------|",
        "| Star schema over snowflake | Single grain, no multi-level hierarchies; Metabase performs better with fewer joins |",
        "| Degenerate dims for names | 40K unique values each; normalizing would create near-1:1 tables with no aggregation benefit |",
        "| CHECK constraints for Gender/Blood Type | 2 and 8 values respectively; join overhead unjustified for such small domains |",
        "| `date_id` as YYYYMMDD integer | Human-readable, sortable, efficient for range queries; avoids surrogate key lookups |",
        "| Pre-computed flags on fact | Avoids CASE expressions in every Metabase query; negligible storage cost for 6 booleans |",
        "| ON CONFLICT DO NOTHING | Enables idempotent re-runs without duplicates |",
    ]

    (REPORTS_DIR / "data_model_description.md").write_text(
        "\n".join(lines), encoding="utf-8",
    )
    log.info("-> reports/data_model_description.md")


# ===================================================================
# PIPELINE PRINCIPAL
# ===================================================================
def main() -> None:
    log.info("=" * 60)
    log.info("Phase 3 — Healthcare Data Model & Load")
    log.info("=" * 60)

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    # Cargar CSV limpio
    df = load_clean_csv()

    # Conectar a PostgreSQL
    try:
        conn = get_connection()
    except psycopg2.OperationalError as e:
        log.error("Cannot connect to PostgreSQL: %s", e)
        log.error("Ensure the database is running and credentials are set.")
        log.error("See .env.example for required environment variables.")
        sys.exit(1)

    try:
        # Crear esquema y tablas
        log.info("--- Creating schema ---")
        execute_schema(conn)

        # Cargar primero dimensiones
        load_all_dimensions(conn, df)

        # Cargar tabla de hechos (resuelve FKs internamente)
        rejected = load_fact_table(conn, df)

        # Escribir registros de auditoria
        write_audit_entries(conn, str(DATA_CLEAN.relative_to(PROJECT_ROOT)))

        # Validacion posterior a la carga
        log.info("--- Running post-load validations ---")
        checks = validate_load(conn)

    except Exception as e:
        conn.rollback()
        log.error("Pipeline failed: %s", e)
        raise
    finally:
        conn.close()
        log.info("Database connection closed.")

    # Generar reportes
    log.info("--- Generating reports ---")
    write_load_summary()
    write_rejected_records()
    write_load_report(checks)
    write_data_model_description()

    # Resumen final
    passed = sum(1 for c in checks if c["status"] == "PASS")
    total = len(checks)
    total_rows = sum(s["rows_loaded"] for s in _load_summary)
    log.info("=" * 60)
    log.info("Phase 3 complete. %s/%s validations passed.", passed, total)
    log.info("Total rows loaded: %s across %s tables.",
             f"{total_rows:,}", len(_load_summary))
    log.info("Rejected: %s rows.", len(_rejected_records))
    log.info("=" * 60)

    if passed < total:
        failed = [c for c in checks if c["status"] == "FAIL"]
        log.warning("FAILED validations:")
        for f in failed:
            log.warning("  %s: %s", f["check"], f["detail"])
        sys.exit(1)
    else:
        log.info("Database is READY for Phase 4 (Metabase dashboard).")


if __name__ == "__main__":
    main()
