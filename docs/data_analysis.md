# Análisis de Datos y Diagnóstico de Calidad

Documento técnico que describe el proceso de exploración, los hallazgos de calidad de datos y las decisiones de limpieza aplicadas al dataset de admisiones hospitalarias.

---

## 1. Dataset Original

| Atributo | Valor |
|----------|-------|
| **Fuente** | [Kaggle: prasad22/healthcare-dataset](https://www.kaggle.com/datasets/prasad22/healthcare-dataset) |
| **Registros** | 55,500 |
| **Columnas** | 15 |
| **Período** | Mayo 2019 – Junio 2024 |
| **Naturaleza** | Datos sintéticos generados con Faker |

### Esquema Original

| Columna | Tipo | Descripción |
|---------|------|-------------|
| Name | String | Nombre del paciente |
| Age | Integer | Edad (13-89 años) |
| Gender | String | Male/Female |
| Blood Type | String | 8 tipos (A+, A-, B+, B-, AB+, AB-, O+, O-) |
| Medical Condition | String | 6 condiciones (Arthritis, Asthma, Cancer, Diabetes, Hypertension, Obesity) |
| Date of Admission | Date | Fecha de ingreso |
| Doctor | String | Nombre del médico |
| Hospital | String | Nombre del hospital |
| Insurance Provider | String | 5 aseguradoras |
| Billing Amount | Float | Monto facturado (USD) |
| Room Number | Integer | Número de habitación (100-500) |
| Admission Type | String | Elective/Emergency/Urgent |
| Discharge Date | Date | Fecha de alta |
| Medication | String | 5 medicamentos |
| Test Results | String | Normal/Abnormal/Inconclusive |

---

## 2. Diagnóstico de Calidad

### 2.1 Problemas Identificados

| # | Problema | Severidad | Evidencia | Impacto |
|---|----------|-----------|-----------|---------|
| 1 | 534 duplicados exactos | **Alta** | Coincidencia en las 15 columnas | Sesgo en métricas de volumen |
| 2 | ~5,500 cuasi-duplicados | **Media** | Mismo paciente, fechas distintas | Posibles readmisiones legítimas |
| 3 | 99.94% de nombres sin Title Case | **Alta** | Capitalización errática | Agrupaciones incorrectas |
| 4 | 42% de hospitales malformados | **Alta** | Comas finales, artefactos "and" | Nombres inconsistentes |
| 5 | Facturación con 15 decimales | **Media** | Precisión incompatible | Formato monetario inválido |
| 6 | 108 registros facturación negativa | **Media** | Valores <0 sin justificación | Posibles ajustes/errores |
| 7 | 40 registros facturación < $100 | **Baja** | Valores atípicos vs. media $25,500 | Anomalía estadística |
| 8 | Dataset sintético (Faker) | **Info** | Distribuciones uniformes | Sin patrones reales |

### 2.2 Análisis de Nulos

| Columna | Nulos | Porcentaje |
|---------|-------|------------|
| Todas | 0 | 0.0% |

**Conclusión:** Dataset completamente poblado, sin valores faltantes.

### 2.3 Análisis de Duplicados

| Tipo | Cantidad | Criterio |
|------|----------|----------|
| Duplicados exactos | 534 | Coincidencia en 15 columnas |
| Cuasi-duplicados | ~5,500 | Mismo Name + Doctor + Hospital |

**Decisión:** Eliminar duplicados exactos. Conservar cuasi-duplicados como posibles readmisiones.

### 2.4 Análisis Numérico

| Variable | Min | Max | Media | Mediana | Std |
|----------|-----|-----|-------|---------|-----|
| Age | 13 | 89 | 51.4 | 52 | 22.1 |
| Billing Amount | -1,000 | 52,764 | 25,517 | 25,516 | 14,736 |
| Room Number | 100 | 500 | 300 | 300 | 115 |
| Stay Duration | 1 | 30 | 15.5 | 16 | 8.6 |

### 2.5 Análisis Categórico

| Variable | Cardinalidad | Distribución |
|----------|--------------|--------------|
| Gender | 2 | ~50% cada uno |
| Blood Type | 8 | ~12.5% cada uno |
| Medical Condition | 6 | ~16.7% cada uno |
| Insurance Provider | 5 | ~20% cada uno |
| Admission Type | 3 | ~33.3% cada uno |
| Test Results | 3 | ~33.3% cada uno |
| Medication | 5 | ~20% cada uno |
| Hospital | ~40,000 | Alta cardinalidad |
| Doctor | ~2,200 | Alta cardinalidad |

**Patrón detectado:** Distribuciones uniformes consistentes con generación sintética (Faker).

---

## 3. Reglas de Limpieza Aplicadas

| ID | Regla | Columnas Afectadas | Filas Afectadas |
|----|-------|-------------------|-----------------|
| R1 | Eliminación de duplicados exactos | Todas | 534 eliminadas |
| R2 | Generación de clave subrogada | admission_id (nueva) | 54,966 asignadas |
| R3 | Normalización de nombres de pacientes | Name | ~55,467 |
| R4 | Normalización de nombres de doctores | Doctor | ~2,192 |
| R5 | Limpieza de hospitales | Hospital | ~27,597 |
| R6 | Redondeo de billing a 2 decimales | Billing Amount | 54,966 |
| R7 | Indicador de facturación negativa | is_billing_negative (nueva) | 106 marcados |
| R8 | Indicador de facturación baja | is_billing_low (nueva) | 40 marcados |
| R9 | Validación de coherencia de fechas | Date of Admission, Discharge Date | 0 violaciones |
| R10 | Recorte y colapso de espacios | Campos texto | Defensivo |

---

## 4. Columnas Derivadas

| Columna | Tipo | Lógica | Uso Analítico |
|---------|------|--------|---------------|
| admission_id | String | UUID v4 | Clave primaria |
| stay_duration_days | Integer | discharge - admission | Análisis de estancia |
| age_group | String | 6 bandas (13-17, 18-30, etc.) | Segmentación |
| is_pediatric | Boolean | age < 18 | Filtro clínico |
| billing_range | String | Cuartiles | Distribución facturación |
| admission_month | String | YYYY-MM | Series temporales |
| admission_year | Integer | Año | Filtro global |
| admission_quarter | Integer | 1-4 | Agregación temporal |
| abnormal_test_flag | Boolean | Test Results == "Abnormal" | Tasa anormalidad |
| is_long_stay | Boolean | stay > P75 (23 días) | Indicador operativo |
| is_billing_negative | Boolean | billing < 0 | Indicador de calidad |
| is_billing_low | Boolean | billing < 100 | Indicador de calidad |
| negative_outcome_flag | Boolean | abnormal AND billing_neg | Riesgo compuesto |
| name_clean | String | Title Case | Normalizado |
| doctor_clean | String | Title Case | Normalizado |
| hospital_clean | String | Title Case, sin artefactos | Normalizado |

---

## 5. Validaciones Post-Transformación

| # | Validación | Resultado |
|---|-----------|-----------|
| 1 | Conteo de filas = 54,966 | ✅ APROBADO |
| 2 | Sin duplicados en admission_id | ✅ APROBADO |
| 3 | Todas las edades en rango [13, 89] | ✅ APROBADO |
| 4 | Fechas de admisión <= fechas de alta | ✅ APROBADO |
| 5 | stay_duration_days >= 1 | ✅ APROBADO |
| 6 | Gender en {Male, Female} | ✅ APROBADO |
| 7 | Blood Type en 8 valores válidos | ✅ APROBADO |
| 8 | Medical Condition en 6 valores | ✅ APROBADO |
| 9 | Insurance Provider en 5 valores | ✅ APROBADO |
| 10 | Admission Type en 3 valores | ✅ APROBADO |
| 11 | Test Results en 3 valores | ✅ APROBADO |
| 12 | Medication en 5 valores | ✅ APROBADO |
| 13 | is_billing_negative correctamente asignado | ✅ APROBADO |
| 14 | is_billing_low correctamente asignado | ✅ APROBADO |
| 15 | age_group cubre todas las edades | ✅ APROBADO |
| 16 | billing_range asignado a todos | ✅ APROBADO |
| 17 | admission_month formato YYYY-MM | ✅ APROBADO |
| 18 | admission_quarter en {1,2,3,4} | ✅ APROBADO |
| 19 | is_pediatric coherente con age | ✅ APROBADO |
| 20 | abnormal_test_flag coherente | ✅ APROBADO |
| 21 | Sin nulos en columnas críticas | ✅ APROBADO |
| 22 | Integridad de datos preservada | ✅ APROBADO |

**Total:** 22 validaciones ejecutadas, 22 aprobadas.

---

## 6. Métricas de Calidad Antes/Después

| Métrica | Antes | Después | Mejora |
|---------|-------|---------|--------|
| Registros totales | 55,500 | 54,966 | -534 duplicados |
| Duplicados exactos | 534 | 0 | 100% eliminados |
| Nombres normalizados | 0% | 100% | +100% |
| Hospitales limpios | 58% | 100% | +42% |
| Billing con 2 decimales | 0% | 100% | +100% |
| Indicadores de calidad | 0 | 3 | +3 columnas |
| Columnas derivadas | 0 | 16 | +16 columnas |

---

## 7. Riesgos Residuales

| Riesgo | Severidad | Mitigación |
|--------|-----------|------------|
| Cuasi-duplicados no diferenciables | Media | Documentado; conservados |
| Facturación negativa sin explicación | Baja | Indicador creado; excluible en consultas |
| Alta cardinalidad hospitales | Baja | Dimensión degenerada en tabla de hechos |
| Datos sintéticos | Info | Documentado; no afecta arquitectura |

---

## 8. Referencias

- Dataset: [Kaggle Healthcare Dataset](https://www.kaggle.com/datasets/prasad22/healthcare-dataset)
- Reporte EDA completo: `reports/eda_report.md`
- Diagnóstico JSON: `reports/diagnosis_report.json`
- Log de transformaciones: `reports/transformation_log.csv`
