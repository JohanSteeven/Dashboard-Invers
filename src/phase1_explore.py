"""
Phase 1 — Exploracion y Diagnostico del Dataset
Healthcare Admissions Dataset (55,500 rows x 15 columns)

Genera reportes de EDA, diagnosticos de calidad de datos y visualizaciones basicas.
Todos los resultados se guardan en reports/ y reports/figures/.
"""

import json
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")  # backend no interactivo para ejecucion sin interfaz
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
import seaborn as sns

# ---------------------------------------------------------------------------
# Rutas
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_RAW = PROJECT_ROOT / "data" / "raw" / "healthcare_dataset.csv"
REPORTS_DIR = PROJECT_ROOT / "reports"
FIGURES_DIR = REPORTS_DIR / "figures"

DATE_COLUMNS = ["Date of Admission", "Discharge Date"]
NUMERIC_COLUMNS = ["Age", "Billing Amount", "Room Number"]
CATEGORICAL_COLUMNS = [
    "Gender",
    "Blood Type",
    "Medical Condition",
    "Insurance Provider",
    "Admission Type",
    "Medication",
    "Test Results",
]
HIGH_CARD_TEXT = ["Name", "Doctor", "Hospital"]


# ---------------------------------------------------------------------------
# 1. Carga
# ---------------------------------------------------------------------------
def load_data(path: Path) -> pd.DataFrame:
    """Lee el CSV crudo y parsea columnas de fecha."""
    if not path.exists():
        print(f"[ERROR] File not found: {path}")
        sys.exit(1)

    df = pd.read_csv(path)
    for col in DATE_COLUMNS:
        df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


# ---------------------------------------------------------------------------
# 2. Resumen estructural
# ---------------------------------------------------------------------------
def structural_overview(df: pd.DataFrame) -> dict:
    """Forma basica, tipos y uso de memoria."""
    return {
        "rows": int(df.shape[0]),
        "columns": int(df.shape[1]),
        "memory_mb": round(df.memory_usage(deep=True).sum() / 1e6, 2),
        "column_types": {col: str(dtype) for col, dtype in df.dtypes.items()},
    }


# ---------------------------------------------------------------------------
# 3. Analisis de nulos
# ---------------------------------------------------------------------------
def null_analysis(df: pd.DataFrame) -> pd.DataFrame:
    """Conteos absolutos y porcentuales de nulos por columna."""
    nulls = df.isnull().sum()
    pct = (nulls / len(df) * 100).round(2)
    summary = pd.DataFrame({"null_count": nulls, "null_pct": pct})
    summary = summary.sort_values("null_count", ascending=False)
    return summary


# ---------------------------------------------------------------------------
# 4. Deteccion de duplicados
# ---------------------------------------------------------------------------
def duplicate_analysis(df: pd.DataFrame) -> dict:
    """Duplicados exactos y casi-duplicados sospechosos."""
    exact_mask = df.duplicated(keep="first")
    exact_count = int(exact_mask.sum())

    # Duplicados sospechosos: mismo Name + Date of Admission + Doctor + Hospital
    # pero diferentes en al menos otra columna
    suspect_cols = ["Name", "Date of Admission", "Doctor", "Hospital"]
    available = [c for c in suspect_cols if c in df.columns]
    if len(available) == len(suspect_cols):
        group_sizes = df.groupby(available).size()
        suspect_groups = group_sizes[group_sizes > 1]
        suspect_count = int(suspect_groups.sum() - len(suspect_groups))
    else:
        suspect_count = 0

    return {
        "exact_duplicates": exact_count,
        "exact_pct": round(exact_count / len(df) * 100, 2),
        "suspicious_near_duplicates": suspect_count,
        "rows_after_dedup": int(len(df) - exact_count),
    }


# ---------------------------------------------------------------------------
# 5. Cardinalidad
# ---------------------------------------------------------------------------
def cardinality_report(df: pd.DataFrame) -> pd.DataFrame:
    """Valores unicos por columna."""
    card = df.nunique()
    pct = (card / len(df) * 100).round(2)
    report = pd.DataFrame({
        "unique_values": card,
        "unique_pct": pct,
        "sample_values": [df[c].dropna().unique()[:5].tolist() for c in df.columns],
    })
    return report.sort_values("unique_values", ascending=False)


# ---------------------------------------------------------------------------
# 6. Perfilado numerico
# ---------------------------------------------------------------------------
def numerical_profiling(df: pd.DataFrame) -> pd.DataFrame:
    """Estadisticas descriptivas + deteccion de outliers (metodo IQR) para columnas numericas."""
    num_cols = [c for c in NUMERIC_COLUMNS if c in df.columns]
    rows = []
    for col in num_cols:
        s = df[col].dropna()
        q1, q3 = s.quantile(0.25), s.quantile(0.75)
        iqr = q3 - q1
        lower_fence = q1 - 1.5 * iqr
        upper_fence = q3 + 1.5 * iqr
        outliers = s[(s < lower_fence) | (s > upper_fence)]
        rows.append({
            "column": col,
            "count": int(s.count()),
            "mean": round(float(s.mean()), 2),
            "std": round(float(s.std()), 2),
            "min": round(float(s.min()), 2),
            "q1": round(float(q1), 2),
            "median": round(float(s.median()), 2),
            "q3": round(float(q3), 2),
            "max": round(float(s.max()), 2),
            "iqr": round(float(iqr), 2),
            "lower_fence": round(float(lower_fence), 2),
            "upper_fence": round(float(upper_fence), 2),
            "outlier_count": int(len(outliers)),
            "outlier_pct": round(len(outliers) / len(s) * 100, 2),
            "negative_values": int((s < 0).sum()),
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# 7. Perfilado categorico
# ---------------------------------------------------------------------------
def categorical_profiling(df: pd.DataFrame) -> pd.DataFrame:
    """Tablas de frecuencia para columnas categoricas de baja cardinalidad."""
    rows = []
    for col in CATEGORICAL_COLUMNS:
        if col not in df.columns:
            continue
        vc = df[col].value_counts()
        for val, count in vc.items():
            rows.append({
                "column": col,
                "value": val,
                "count": int(count),
                "pct": round(count / len(df) * 100, 2),
            })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# 8. Validacion de fechas
# ---------------------------------------------------------------------------
def date_analysis(df: pd.DataFrame) -> dict:
    """Detecta fechas invalidas, faltantes o incoherentes."""
    results = {}
    for col in DATE_COLUMNS:
        if col not in df.columns:
            continue
        nat_count = int(df[col].isna().sum())
        valid = df[col].dropna()
        results[col] = {
            "nat_count": nat_count,
            "min": str(valid.min().date()) if len(valid) > 0 else None,
            "max": str(valid.max().date()) if len(valid) > 0 else None,
            "unique_dates": int(valid.nunique()),
        }

    # Coherencia entre fechas: Discharge debe ser >= Date of Admission
    if "Date of Admission" in df.columns and "Discharge Date" in df.columns:
        mask = df["Discharge Date"] < df["Date of Admission"]
        invalid_range = int(mask.sum())
        results["discharge_before_admission"] = invalid_range

    # Distribucion de duracion de estancia
    if "Date of Admission" in df.columns and "Discharge Date" in df.columns:
        los = (df["Discharge Date"] - df["Date of Admission"]).dt.days
        results["length_of_stay"] = {
            "min": int(los.min()) if not los.isna().all() else None,
            "max": int(los.max()) if not los.isna().all() else None,
            "mean": round(float(los.mean()), 2) if not los.isna().all() else None,
            "zero_day_stays": int((los == 0).sum()),
        }
    return results


# ---------------------------------------------------------------------------
# 9. Inconsistencias de texto
# ---------------------------------------------------------------------------
def text_quality_analysis(df: pd.DataFrame) -> dict:
    """Detecta problemas de capitalizacion, espacios al inicio/final y patrones sucios."""
    results = {}
    for col in HIGH_CARD_TEXT:
        if col not in df.columns:
            continue
        s = df[col].dropna().astype(str)
        total = len(s)

        leading_trailing_spaces = int(s.str.contains(r"^\s|\s$", regex=True).sum())
        multiple_spaces = int(s.str.contains(r"\s{2,}", regex=True).sum())

        # Verificacion de Title Case
        title_case_mask = s.apply(lambda x: x == x.title())
        not_title_case = int((~title_case_mask).sum())

        info = {
            "total": total,
            "leading_trailing_spaces": leading_trailing_spaces,
            "multiple_spaces": multiple_spaces,
            "not_title_case": not_title_case,
            "not_title_case_pct": round(not_title_case / total * 100, 2) if total > 0 else 0,
        }

        # Verificaciones especificas por columna
        if col == "Name":
            prefixes = ["Mr.", "Mrs.", "Ms.", "Dr.", "Miss"]
            prefix_mask = s.str.split().str[0].isin(prefixes)
            info["with_prefix"] = int(prefix_mask.sum())

        if col == "Hospital":
            info["contains_comma"] = int(s.str.contains(",").sum())
            info["starts_with_and"] = int(s.str.startswith("and ").sum())
            info["ends_with_and"] = int(s.str.endswith(" and").sum())
            info["ends_with_comma"] = int(s.str.endswith(",").sum())

        if col == "Doctor":
            suffixes = ["MD", "DVM", "PhD", "Jr.", "Sr."]
            suffix_mask = s.str.split().str[-1].isin(suffixes)
            info["with_suffix"] = int(suffix_mask.sum())
            prefix_mask = s.str.split().str[0].isin(["Mr.", "Mrs.", "Ms.", "Dr.", "Miss"])
            info["with_prefix"] = int(prefix_mask.sum())

        results[col] = info
    return results


# ---------------------------------------------------------------------------
# 10. Resumen de prioridades de calidad
# ---------------------------------------------------------------------------
def quality_priority_summary(
    struct: dict,
    nulls_df: pd.DataFrame,
    dupes: dict,
    num_prof: pd.DataFrame,
    text_quality: dict,
    date_info: dict,
) -> list[dict]:
    """Lista consolidada de problemas de calidad de datos, ordenada por severidad."""
    issues = []

    # Duplicados
    if dupes["exact_duplicates"] > 0:
        issues.append({
            "priority": 1,
            "severity": "HIGH",
            "issue": "Exact duplicate rows",
            "detail": f"{dupes['exact_duplicates']} rows ({dupes['exact_pct']}%)",
            "action": "Remove exact duplicates",
        })

    # Sin llave primaria (siempre cierto para este dataset)
    issues.append({
        "priority": 2,
        "severity": "HIGH",
        "issue": "No natural primary key",
        "detail": "No column or combination guarantees uniqueness",
        "action": "Generate surrogate key (admission_id)",
    })

    # Nulos
    total_nulls = int(nulls_df["null_count"].sum())
    if total_nulls > 0:
        cols_with_nulls = int((nulls_df["null_count"] > 0).sum())
        issues.append({
            "priority": 3,
            "severity": "HIGH" if total_nulls / struct["rows"] > 0.05 else "MEDIUM",
            "issue": "Missing values",
            "detail": f"{total_nulls} nulls across {cols_with_nulls} columns",
            "action": "Impute or flag depending on column",
        })
    else:
        issues.append({
            "priority": 3,
            "severity": "OBSERVATION",
            "issue": "Zero nulls in all columns",
            "detail": "Suspiciously clean for a 55K-row dataset — consistent with synthetic data",
            "action": "Document as synthetic data indicator",
        })

    # Capitalizacion de nombres
    name_info = text_quality.get("Name", {})
    if name_info.get("not_title_case", 0) > 0:
        issues.append({
            "priority": 4,
            "severity": "MEDIUM",
            "issue": "Erratic capitalization in patient names",
            "detail": f"{name_info['not_title_case']} names ({name_info['not_title_case_pct']}%)",
            "action": "Normalize to Title Case, remove prefixes",
        })

    # Facturacion negativa
    neg_row = num_prof[num_prof["column"] == "Billing Amount"]
    if not neg_row.empty:
        neg_count = int(neg_row.iloc[0]["negative_values"])
        if neg_count > 0:
            issues.append({
                "priority": 5,
                "severity": "MEDIUM",
                "issue": "Negative billing amounts",
                "detail": f"{neg_count} records with negative billing",
                "action": "Flag with is_billing_negative; exclude from revenue metrics",
            })

    # Nombres de hospitales
    hosp_info = text_quality.get("Hospital", {})
    if hosp_info.get("contains_comma", 0) > 0:
        issues.append({
            "priority": 6,
            "severity": "MEDIUM",
            "issue": "Malformed hospital names",
            "detail": (
                f"{hosp_info.get('contains_comma', 0)} with commas, "
                f"{hosp_info.get('starts_with_and', 0)} starting with 'and'"
            ),
            "action": "Clean trailing commas, leading/trailing 'and'",
        })

    # Precision de facturacion
    issues.append({
        "priority": 7,
        "severity": "LOW",
        "issue": "Excessive decimal precision in Billing Amount",
        "detail": "Up to 15 decimals in billing values",
        "action": "Round to 2 decimals",
    })

    # Alta antes del ingreso
    dba = date_info.get("discharge_before_admission", 0)
    if dba > 0:
        issues.append({
            "priority": 8,
            "severity": "HIGH",
            "issue": "Discharge date before admission date",
            "detail": f"{dba} rows with negative length of stay",
            "action": "Investigate and correct or remove",
        })

    # Distribuciones uniformes (observacion)
    issues.append({
        "priority": 9,
        "severity": "OBSERVATION",
        "issue": "Perfectly uniform distributions in all categorical columns",
        "detail": "Consistent with synthetically generated data",
        "action": "Document; comparative analysis between categories will not yield significant differences",
    })

    return sorted(issues, key=lambda x: x["priority"])


# ---------------------------------------------------------------------------
# 11. Constructor de diccionario de datos
# ---------------------------------------------------------------------------
def build_data_dictionary(df: pd.DataFrame) -> pd.DataFrame:
    """Genera un diccionario de datos con metadatos de columnas."""
    rows = []
    for col in df.columns:
        s = df[col]
        dtype = str(s.dtype)
        nunique = int(s.nunique())
        nulls = int(s.isna().sum())

        if pd.api.types.is_numeric_dtype(s):
            role = "Metric" if nunique > 20 else "Dimension"
            sample = f"min={s.min()}, max={s.max()}"
        elif pd.api.types.is_datetime64_any_dtype(s):
            role = "Temporal dimension"
            sample = f"{s.min()} to {s.max()}"
        else:
            role = "Dimension" if nunique <= 20 else "High-cardinality attribute"
            sample = ", ".join(str(v) for v in s.dropna().unique()[:5])

        rows.append({
            "column": col,
            "dtype_inferred": dtype,
            "unique_values": nunique,
            "null_count": nulls,
            "role": role,
            "sample_values": sample,
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# 12. Visualizaciones
# ---------------------------------------------------------------------------
def plot_null_distribution(nulls_df: pd.DataFrame, out_dir: Path) -> None:
    """Grafico de barras con conteo de nulos por columna."""
    fig, ax = plt.subplots(figsize=(12, 5))
    colors = ["#2ecc71" if v == 0 else "#e74c3c" for v in nulls_df["null_count"]]
    ax.barh(nulls_df.index, nulls_df["null_count"], color=colors)
    ax.set_xlabel("Null count")
    ax.set_title("Null Distribution by Column")
    ax.invert_yaxis()
    for i, v in enumerate(nulls_df["null_count"]):
        ax.text(v + 0.5, i, str(v), va="center", fontsize=9)
    fig.tight_layout()
    fig.savefig(out_dir / "null_distribution.png", dpi=150)
    plt.close(fig)


def plot_top_categories(df: pd.DataFrame, col: str, out_dir: Path) -> None:
    """Grafico de barras horizontal de categorias principales para una columna dada."""
    vc = df[col].value_counts().head(15)
    fig, ax = plt.subplots(figsize=(9, 5))
    sns.barplot(x=vc.values, y=vc.index, hue=vc.index, palette="viridis", legend=False, ax=ax)
    ax.set_xlabel("Count")
    ax.set_title(f"Top Categories — {col}")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    plt.tight_layout()
    fig.savefig(out_dir / f"top_categories_{col.lower().replace(' ', '_')}.png", dpi=150)
    plt.close(fig)


def plot_numeric_distribution(df: pd.DataFrame, col: str, out_dir: Path) -> None:
    """Histograma + KDE para una columna numerica."""
    fig, ax = plt.subplots(figsize=(9, 5))
    data = df[col].dropna()
    sns.histplot(data, bins=50, kde=True, color="#3498db", ax=ax)
    ax.axvline(data.mean(), color="#e74c3c", linestyle="--", label=f"Mean: {data.mean():,.2f}")
    ax.axvline(data.median(), color="#2ecc71", linestyle="--", label=f"Median: {data.median():,.2f}")
    ax.set_title(f"Distribution — {col}")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax.legend()
    plt.tight_layout()
    fig.savefig(out_dir / f"distribution_{col.lower().replace(' ', '_')}.png", dpi=150)
    plt.close(fig)


def plot_monthly_admissions(df: pd.DataFrame, date_col: str, out_dir: Path) -> None:
    """Serie temporal mensual del conteo de admisiones."""
    if date_col not in df.columns:
        return
    monthly = df.set_index(date_col).resample("MS").size().reset_index(name="admissions")
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(monthly[date_col], monthly["admissions"], marker="o", markersize=3, color="#2c3e50")
    ax.fill_between(monthly[date_col], monthly["admissions"], alpha=0.1, color="#2c3e50")
    ax.set_title("Monthly Hospital Admissions")
    ax.set_xlabel("Month")
    ax.set_ylabel("Admissions")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    plt.xticks(rotation=45)
    plt.tight_layout()
    fig.savefig(out_dir / "monthly_admissions.png", dpi=150)
    plt.close(fig)


# ---------------------------------------------------------------------------
# 13. Funciones de exportacion
# ---------------------------------------------------------------------------
def export_eda_report(
    struct: dict,
    nulls_df: pd.DataFrame,
    dupes: dict,
    num_prof: pd.DataFrame,
    cat_prof: pd.DataFrame,
    date_info: dict,
    text_quality: dict,
    quality_issues: list[dict],
    out_dir: Path,
) -> None:
    """Escribe un reporte EDA en Markdown."""
    lines = [
        "# EDA Report — Healthcare Dataset",
        f"**Generated:** {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}",
        f"**Source:** `data/raw/healthcare_dataset.csv`",
        "",
        "---",
        "",
        "## 1. Structural Overview",
        "",
        f"- **Rows:** {struct['rows']:,}",
        f"- **Columns:** {struct['columns']}",
        f"- **Memory:** {struct['memory_mb']} MB",
        "",
        "### Column Types",
        "",
        "| Column | Type |",
        "|--------|------|",
    ]
    for col, dtype in struct["column_types"].items():
        lines.append(f"| {col} | `{dtype}` |")

    lines += [
        "",
        "---",
        "",
        "## 2. Null Analysis",
        "",
        f"**Total nulls:** {int(nulls_df['null_count'].sum()):,}",
        "",
        "| Column | Null Count | Null % |",
        "|--------|-----------|--------|",
    ]
    for col, row in nulls_df.iterrows():
        lines.append(f"| {col} | {int(row['null_count'])} | {row['null_pct']}% |")

    lines += [
        "",
        "---",
        "",
        "## 3. Duplicate Analysis",
        "",
        f"- **Exact duplicates:** {dupes['exact_duplicates']} ({dupes['exact_pct']}%)",
        f"- **Suspicious near-duplicates:** {dupes['suspicious_near_duplicates']}",
        f"- **Rows after dedup:** {dupes['rows_after_dedup']:,}",
        "",
        "---",
        "",
        "## 4. Numerical Profiling",
        "",
    ]
    if not num_prof.empty:
        lines.append(num_prof.to_markdown(index=False))
    else:
        lines.append("No numeric columns detected.")

    lines += [
        "",
        "---",
        "",
        "## 5. Date Analysis",
        "",
    ]
    for key, val in date_info.items():
        if isinstance(val, dict):
            lines.append(f"### {key}")
            for k, v in val.items():
                lines.append(f"- **{k}:** {v}")
            lines.append("")
        else:
            lines.append(f"- **{key}:** {val}")
    lines.append("")

    lines += [
        "---",
        "",
        "## 6. Text Quality",
        "",
    ]
    for col, info in text_quality.items():
        lines.append(f"### {col}")
        for k, v in info.items():
            lines.append(f"- **{k}:** {v:,}" if isinstance(v, int) else f"- **{k}:** {v}")
        lines.append("")

    lines += [
        "---",
        "",
        "## 7. Quality Issues (Prioritized)",
        "",
        "| # | Severity | Issue | Detail | Action |",
        "|---|----------|-------|--------|--------|",
    ]
    for issue in quality_issues:
        lines.append(
            f"| {issue['priority']} | {issue['severity']} | {issue['issue']} "
            f"| {issue['detail']} | {issue['action']} |"
        )

    lines += [
        "",
        "---",
        "",
        "## 8. Visualizations",
        "",
        "See `reports/figures/` for generated charts:",
        "- `null_distribution.png`",
        "- `top_categories_medical_condition.png`",
        "- `distribution_billing_amount.png`",
        "- `monthly_admissions.png`",
    ]

    (out_dir / "eda_report.md").write_text("\n".join(lines), encoding="utf-8")


def export_diagnosis_json(
    struct: dict,
    dupes: dict,
    date_info: dict,
    text_quality: dict,
    quality_issues: list[dict],
    out_dir: Path,
) -> None:
    """Exporta el diagnostico estructurado como JSON para consumo posterior."""
    diagnosis = {
        "generated_at": pd.Timestamp.now().isoformat(),
        "source_file": "data/raw/healthcare_dataset.csv",
        "structural_overview": struct,
        "duplicate_analysis": dupes,
        "date_analysis": _serialize_date_info(date_info),
        "text_quality": text_quality,
        "quality_issues": quality_issues,
    }
    (out_dir / "diagnosis_report.json").write_text(
        json.dumps(diagnosis, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )


def _serialize_date_info(date_info: dict) -> dict:
    """Asegura que todos los valores en date_info sean serializables a JSON."""
    out = {}
    for k, v in date_info.items():
        if isinstance(v, dict):
            out[k] = {sk: str(sv) if hasattr(sv, "isoformat") else sv for sk, sv in v.items()}
        else:
            out[k] = str(v) if hasattr(v, "isoformat") else v
    return out


# ---------------------------------------------------------------------------
# Principal
# ---------------------------------------------------------------------------
def main() -> None:
    print("=" * 60)
    print("Phase 1 - Healthcare Dataset EDA")
    print("=" * 60)

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)

    # Carga
    print("\n[1/10] Loading dataset...")
    df = load_data(DATA_RAW)
    print(f"       Loaded {df.shape[0]:,} rows x {df.shape[1]} columns")

    # Estructura
    print("[2/10] Structural overview...")
    struct = structural_overview(df)

    # Nulos
    print("[3/10] Null analysis...")
    nulls_df = null_analysis(df)
    nulls_df.to_csv(REPORTS_DIR / "nulls_summary.csv")

    # Duplicados
    print("[4/10] Duplicate detection...")
    dupes = duplicate_analysis(df)
    dup_df = pd.DataFrame([dupes])
    dup_df.to_csv(REPORTS_DIR / "duplicates_summary.csv", index=False)
    print(f"       Found {dupes['exact_duplicates']} exact duplicates")

    # Cardinalidad
    print("[5/10] Cardinality analysis...")
    card_df = cardinality_report(df)
    # cardinalidad usada internamente; el diccionario de datos cubre esto

    # Perfilado numerico
    print("[6/10] Numerical profiling...")
    num_prof = numerical_profiling(df)
    num_prof.to_csv(REPORTS_DIR / "numerical_summary.csv", index=False)

    # Perfilado categorico
    print("[7/10] Categorical profiling...")
    cat_prof = categorical_profiling(df)
    cat_prof.to_csv(REPORTS_DIR / "categorical_summary.csv", index=False)

    # Fechas
    print("[8/10] Date validation...")
    date_info = date_analysis(df)

    # Calidad de texto
    print("[9/10] Text quality analysis...")
    text_quality = text_quality_analysis(df)

    # Prioridad de calidad
    print("[10/10] Generating quality summary and exports...")
    quality_issues = quality_priority_summary(struct, nulls_df, dupes, num_prof, text_quality, date_info)

    # Diccionario de datos
    dict_df = build_data_dictionary(df)
    dict_df.to_csv(REPORTS_DIR / "data_dictionary.csv", index=False)

    # Reporte en Markdown
    export_eda_report(struct, nulls_df, dupes, num_prof, cat_prof, date_info, text_quality, quality_issues, REPORTS_DIR)

    # Diagnostico JSON
    export_diagnosis_json(struct, dupes, date_info, text_quality, quality_issues, REPORTS_DIR)

    # Visualizaciones
    print("\nGenerating visualizations...")
    plot_null_distribution(nulls_df, FIGURES_DIR)
    print("  -> null_distribution.png")

    plot_top_categories(df, "Medical Condition", FIGURES_DIR)
    print("  -> top_categories_medical_condition.png")

    plot_numeric_distribution(df, "Billing Amount", FIGURES_DIR)
    print("  -> distribution_billing_amount.png")

    plot_monthly_admissions(df, "Date of Admission", FIGURES_DIR)
    print("  -> monthly_admissions.png")

    # Resumen final
    print("\n" + "=" * 60)
    print("Phase 1 complete. Outputs generated:")
    print("=" * 60)
    for f in sorted(REPORTS_DIR.rglob("*")):
        if f.is_file():
            print(f"  {f.relative_to(PROJECT_ROOT)}")
    print(f"\nTotal quality issues found: {len(quality_issues)}")
    high = sum(1 for i in quality_issues if i["severity"] == "HIGH")
    med = sum(1 for i in quality_issues if i["severity"] == "MEDIUM")
    print(f"  HIGH: {high} | MEDIUM: {med} | LOW/OBS: {len(quality_issues) - high - med}")
    print("\nReady for Phase 2 (cleaning & transformation).")


if __name__ == "__main__":
    main()
