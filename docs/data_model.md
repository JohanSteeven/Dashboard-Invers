# Modelo de Datos — Healthcare Analytics

Documentación técnica del modelo dimensional (star schema) implementado en PostgreSQL para el análisis de admisiones hospitalarias.

---

## 1. Visión General

| Aspecto | Descripción |
|---------|-------------|
| **Tipo de modelo** | Star Schema (Kimball) |
| **Base de datos** | PostgreSQL 16 |
| **Esquema** | `healthcare` |
| **Grano** | Una fila = una admisión hospitalaria |
| **Período cubierto** | Mayo 2019 – Junio 2024 |
| **Registros** | 54,966 admisiones |

---

## 2. Diagrama del Modelo

```
                    ┌──────────────────┐
                    │    dim_date      │
                    │  (date_id PK)    │
                    └────────┬─────────┘
                             │
┌──────────────┐   ┌────────┴─────────┐   ┌──────────────────┐
│dim_medical   │   │                  │   │ dim_admission    │
│_condition    │───│ fact_admissions  │───│ _type            │
│(condition_id)│   │  (admission_id   │   │(admission_type_id│
└──────────────┘   │   PK)            │   └──────────────────┘
                   │                  │
┌──────────────┐   │  FK → 6 dims     │   ┌──────────────────┐
│dim_insurance │───│  Métricas:       │───│ dim_medication   │
│(insurance_id)│   │   billing_amount │   │ (medication_id)  │
└──────────────┘   │   age            │   └──────────────────┘
                   │   stay_duration  │
┌──────────────┐   │   room_number    │
│dim_test      │───│  Indicadores(6)  │
│_result       │   │  Dims. degen.    │
│(test_result_id)  │   (name,doctor,  │
└──────────────┘   │    hospital...)  │
                   └──────────────────┘
```

---

## 3. Tablas de Dimensión

### 3.1 dim_date

Dimensión de fechas para análisis temporal.

| Columna | Tipo | Descripción |
|---------|------|-------------|
| date_id | INTEGER PK | Clave subrogada (YYYYMMDD) |
| full_date | DATE | Fecha completa |
| year | INTEGER | Año |
| month | INTEGER | Mes (1-12) |
| day | INTEGER | Día del mes |
| quarter | INTEGER | Trimestre (1-4) |
| day_of_week | INTEGER | Día de la semana (1=Lunes) |
| week_of_year | INTEGER | Semana del año |
| is_weekend | BOOLEAN | ¿Es fin de semana? |

**Registros:** ~1,860 (fechas únicas en el período)

### 3.2 dim_medical_condition

Dimensión de condiciones médicas.

| Columna | Tipo | Descripción |
|---------|------|-------------|
| condition_id | INTEGER PK | Clave subrogada |
| condition_name | VARCHAR(100) | Nombre de la condición |

**Registros:** 6 (Arthritis, Asthma, Cancer, Diabetes, Hypertension, Obesity)

### 3.3 dim_insurance

Dimensión de proveedores de seguros.

| Columna | Tipo | Descripción |
|---------|------|-------------|
| insurance_id | INTEGER PK | Clave subrogada |
| provider_name | VARCHAR(100) | Nombre de la aseguradora |

**Registros:** 5 (Aetna, Blue Cross, Cigna, Medicare, UnitedHealthcare)

### 3.4 dim_admission_type

Dimensión de tipos de admisión.

| Columna | Tipo | Descripción |
|---------|------|-------------|
| admission_type_id | INTEGER PK | Clave subrogada |
| admission_type_name | VARCHAR(50) | Tipo de admisión |

**Registros:** 3 (Elective, Emergency, Urgent)

### 3.5 dim_medication

Dimensión de medicamentos.

| Columna | Tipo | Descripción |
|---------|------|-------------|
| medication_id | INTEGER PK | Clave subrogada |
| medication_name | VARCHAR(100) | Nombre del medicamento |

**Registros:** 5 (Aspirin, Ibuprofen, Lipitor, Paracetamol, Penicillin)

### 3.6 dim_test_result

Dimensión de resultados de pruebas.

| Columna | Tipo | Descripción |
|---------|------|-------------|
| test_result_id | INTEGER PK | Clave subrogada |
| result_name | VARCHAR(50) | Resultado del test |

**Registros:** 3 (Normal, Abnormal, Inconclusive)

---

## 4. Tabla de Hechos

### fact_admissions

| Columna | Tipo | Descripción |
|---------|------|-------------|
| **admission_id** | VARCHAR(36) PK | UUID de la admisión |
| admission_date_id | INTEGER FK | → dim_date |
| discharge_date_id | INTEGER FK | → dim_date |
| condition_id | INTEGER FK | → dim_medical_condition |
| insurance_id | INTEGER FK | → dim_insurance |
| admission_type_id | INTEGER FK | → dim_admission_type |
| medication_id | INTEGER FK | → dim_medication |
| test_result_id | INTEGER FK | → dim_test_result |
| **patient_name** | VARCHAR(200) | Nombre del paciente (degenerada) |
| **doctor_name** | VARCHAR(200) | Nombre del doctor (degenerada) |
| **hospital_name** | VARCHAR(200) | Nombre del hospital (degenerada) |
| **gender** | VARCHAR(10) | Género del paciente |
| **blood_type** | VARCHAR(5) | Tipo de sangre |
| **age** | INTEGER | Edad al momento de admisión |
| **age_group** | VARCHAR(10) | Banda de edad |
| **room_number** | INTEGER | Número de habitación |
| **billing_amount** | NUMERIC(12,2) | Monto facturado |
| **billing_range** | VARCHAR(20) | Rango de facturación |
| **stay_duration_days** | INTEGER | Días de estancia |
| **is_pediatric** | BOOLEAN | Paciente pediátrico (<18) |
| **is_long_stay** | BOOLEAN | Estancia prolongada (>P75) |
| **is_billing_negative** | BOOLEAN | Billing < 0 |
| **is_billing_low** | BOOLEAN | Billing < $100 |
| **abnormal_test_flag** | BOOLEAN | Resultado anormal |
| **negative_outcome_flag** | BOOLEAN | Riesgo compuesto |

**Registros:** 54,966

---

## 5. Dimensiones Degeneradas

Las siguientes columnas se mantienen en la fact table como **dimensiones degeneradas** debido a su alta cardinalidad y ratio 1:1 con los hechos:

| Columna | Cardinalidad | Razón |
|---------|--------------|-------|
| patient_name | ~54,500 | Casi único por admisión |
| doctor_name | ~2,200 | Promedio 25 admisiones/doctor |
| hospital_name | ~40,000 | Promedio 1.4 admisiones/hospital |
| gender | 2 | Cardinalidad muy baja, no justifica FK |
| blood_type | 8 | Cardinalidad baja, uso limitado |

**Decisión:** Crear tablas de dimensión para estos campos agregaría complejidad sin beneficio analítico.

---

## 6. Índices

| Índice | Tabla | Columnas | Propósito |
|--------|-------|----------|-----------|
| pk_fact_admissions | fact_admissions | admission_id | Clave primaria |
| idx_fact_admission_date | fact_admissions | admission_date_id | Filtro por fecha |
| idx_fact_discharge_date | fact_admissions | discharge_date_id | Análisis de altas |
| idx_fact_condition | fact_admissions | condition_id | Filtro por condición |
| idx_fact_insurance | fact_admissions | insurance_id | Filtro por aseguradora |
| idx_fact_admission_type | fact_admissions | admission_type_id | Filtro por tipo |
| idx_fact_medication | fact_admissions | medication_id | Filtro por medicamento |
| idx_fact_test_result | fact_admissions | test_result_id | Filtro por resultado |
| idx_fact_billing | fact_admissions | billing_amount | Ordenar por facturación |
| idx_fact_age_group | fact_admissions | age_group | Segmentación demográfica |
| idx_fact_hospital | fact_admissions | hospital_name | Búsqueda por hospital |

---

## 7. Vistas Analíticas

### 7.1 v_monthly_admissions

Volumen y métricas por mes.

```sql
SELECT
    year, month, year_month,
    admission_count,
    total_billing,
    avg_billing,
    avg_los_days
FROM healthcare.v_monthly_admissions;
```

### 7.2 v_condition_summary

Métricas por condición médica.

```sql
SELECT
    condition_name,
    admission_count,
    avg_billing,
    avg_los_days,
    abnormal_rate_pct,
    long_stay_count
FROM healthcare.v_condition_summary;
```

### 7.3 v_insurance_summary

Métricas por aseguradora.

```sql
SELECT
    provider_name,
    admission_count,
    total_billing,
    avg_billing,
    market_share_pct
FROM healthcare.v_insurance_summary;
```

### 7.4 v_hospital_ranking

Top hospitales por facturación.

```sql
SELECT
    hospital_name,
    admission_count,
    total_billing,
    avg_billing,
    avg_los_days
FROM healthcare.v_hospital_ranking
LIMIT 10;
```

### 7.5 v_age_distribution

Distribución por grupo de edad.

```sql
SELECT
    age_group,
    admission_count,
    pct_of_total,
    avg_billing,
    pediatric_count
FROM healthcare.v_age_distribution;
```

---

## 8. Constraints y Validaciones

### Claves Foráneas

Todas las FK en fact_admissions tienen `ON DELETE RESTRICT` para proteger integridad referencial.

### Check Constraints

| Constraint | Tabla | Regla |
|------------|-------|-------|
| chk_age_range | fact_admissions | age BETWEEN 13 AND 89 |
| chk_stay_positive | fact_admissions | stay_duration_days >= 1 |
| chk_room_range | fact_admissions | room_number BETWEEN 100 AND 500 |

### Validaciones Post-Carga

| # | Validación | Query |
|---|-----------|-------|
| 1 | Conteo de filas | `SELECT COUNT(*) = 54966 FROM fact_admissions` |
| 2 | Integridad referencial | `LEFT JOIN` sin huérfanos |
| 3 | Rangos de fechas | `MIN/MAX(admission_date)` coherentes |
| 4 | Claves únicas | `COUNT(DISTINCT admission_id) = COUNT(*)` |
| 5 | Vistas ejecutables | Todas las vistas retornan datos |
| 6 | Billing validado | `SUM > 0`, `AVG coherente` |

---

## 9. Decisiones de Diseño

### ¿Por qué Star Schema?

1. **Consultas simples:** Las consultas del dashboard son agregaciones por dimensiones categóricas
2. **Rendimiento:** Minimiza JOINs; consulta típica usa 1-3 joins
3. **Optimizado para BI:** Herramientas como Metabase prefieren esquemas denormalizados
4. **Dimensiones pequeñas:** 3-6 valores por dimensión → no snowflake

### ¿Por qué UUID como clave primaria?

1. **Idempotencia:** Permite recargas sin duplicar
2. **Trazabilidad:** Vincula origen entre fases
3. **Sin secuencias:** Evita conflictos en cargas paralelas (futuro)

### ¿Por qué dimensiones degeneradas?

1. **Hospital:** ~40,000 valores únicos, ratio 1:1.4 con hechos
2. **Doctor:** ~2,200 valores, pero sin atributos adicionales
3. **Paciente:** Casi único por admisión

Crear dimensiones para estos campos:
- Aumentaría complejidad sin reducir almacenamiento
- No habilitaría agregaciones útiles
- Impactaría rendimiento de consultas

---

## 10. Referencias

- DDL completo: `sql/01_schema.sql`
- Queries del dashboard: `sql/02_dashboard_queries.sql`
- Reporte de carga: `reports/load_report.md`
