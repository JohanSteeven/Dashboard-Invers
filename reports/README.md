# Reportes Generados — Healthcare Analytics

Este directorio contiene todos los artefactos de evidencia generados automáticamente por las fases del pipeline.

---

## Índice de Archivos

### Fase 1: Exploración y Diagnóstico

| Archivo | Descripción | Formato |
|---------|-------------|---------|
| `eda_report.md` | Reporte EDA completo: estructura, nulos, duplicados, análisis | Markdown |
| `diagnosis_report.json` | Diagnóstico de calidad máquina-legible con severidades | JSON |
| `data_dictionary.csv` | Diccionario de las 15 columnas originales | CSV |
| `nulls_summary.csv` | Resumen de nulos por columna | CSV |
| `duplicates_summary.csv` | Análisis de duplicados exactos y cuasi-duplicados | CSV |
| `numerical_summary.csv` | Estadísticas descriptivas de columnas numéricas | CSV |
| `categorical_summary.csv` | Distribuciones de frecuencia de columnas categóricas | CSV |


### Fase 2: Limpieza y Transformación

| Archivo | Descripción | Formato |
|---------|-------------|---------|
| `transformation_report.md` | 10 reglas de limpieza documentadas, 22 validaciones | Markdown |
| `transformation_log.csv` | Log trazable de 25 transformaciones aplicadas | CSV |
| `data_quality_before_after.csv` | Métricas de calidad antes y después | CSV |
| `derived_columns_dictionary.csv` | Diccionario de 16 columnas derivadas | CSV |
| `invalid_records.csv` | Registros con indicadores de calidad (facturación negativa/baja) | CSV |

### Fase 3: Modelado y Carga

| Archivo | Descripción | Formato |
|---------|-------------|---------|
| `load_report.md` | Reporte de carga y validaciones post-load (7 checks) | Markdown |
| `load_summary.csv` | Resumen cuantitativo: filas por tabla, tiempos | CSV |
| `data_model_description.md` | Descripción del modelo dimensional (star schema) | Markdown |

---


## Regenerar Reportes

Los reportes se regeneran automáticamente al ejecutar las fases:

```bash
python src/phase1_explore.py    # Regenera reportes de Fase 1
python src/phase2_transform.py  # Regenera reportes de Fase 2
python src/phase3_load.py       # Regenera reportes de Fase 3
```

---

## Notas

- Todos los archivos CSV usan codificación UTF-8 y separador coma
- Los reportes Markdown están formateados para visualización en GitHub
- El `diagnosis_report.json` es consumible por sistemas automatizados
