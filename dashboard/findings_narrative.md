# Narrativa de Hallazgos — Healthcare Admissions Analytics

> **Nota metodológica:** Este dataset es sintéticamente generado (Kaggle — prasad22/healthcare-dataset). Las distribuciones uniformes y la ausencia de correlaciones entre variables son consistentes con datos producidos mediante la librería Faker. Los hallazgos descritos a continuación son técnicamente correctos y demostrables, pero no deben interpretarse como patrones clínicos reales.

## Hallazgos Principales

### Volumen y tendencia temporal (Q1)
El dataset contiene **54,966 admisiones hospitalarias** entre mayo 2019 y junio 2024 (61 meses). El volumen mensual se mantiene estable en torno a **910 admisiones/mes** (σ ≈ 105), sin estacionalidad identificable. Las caídas observadas en los extremos del rango temporal (mayo 2019 y mayo-junio 2024) corresponden a meses con datos parciales, no a variaciones operativas.

### Concentración de facturación (Q2)
La facturación total acumulada (excluyendo 108 registros con facturación negativa) asciende a aproximadamente **$1.4B**. Dado que el dataset contiene ~40,000 hospitales únicos con un promedio de 1.4 admisiones por hospital, el ranking Top 10 refleja varianza estadística más que concentración operativa real. La facturación promedio por admisión es de **~$25,500**.

### Duración de estancia (Q3)
La estancia promedio es de **~15.5 días**, con mediana equivalente, consistente con una distribución uniforme de 1 a 30 días. No se detectan diferencias significativas entre condiciones médicas. Aproximadamente el **25%** de las admisiones se clasifican como estancias prolongadas (>23 días).

### Tasa de resultados anormales (Q4)
La tasa de resultados de test anormales es del **33.6%** globalmente, consistente con una distribución equiprobable entre las tres categorías (Normal, Abnormal, Inconclusive). La matriz cruzada condición × aseguradora muestra valores homogéneos sin patrones diferenciadores.

### Facturación por aseguradora y condición (Q5)
El costo promedio de atención por admisión es uniforme (**~$25,500**) a través de las 5 aseguradoras y 6 condiciones médicas. En un escenario real, este cruce sería fundamental para la negociación de contratos; en este dataset, confirma la naturaleza uniforme de la generación de datos.

## Observación sobre calidad de datos
El pipeline de limpieza (Fase 2) procesó exitosamente las 10 reglas de transformación identificadas en la Fase 1:
- Eliminación de **534 duplicados** exactos
- Normalización de **~55K nombres** de pacientes y doctores
- Limpieza de **~42% de hospitales** malformados
- Redondeo de facturación a 2 decimales
- Generación de **16 columnas derivadas** con indicadores operativos

El modelo dimensional cargado en PostgreSQL (1 fact table, 6 dimensiones, 5 vistas analíticas) soporta todas las consultas del dashboard sin transformaciones adicionales.

