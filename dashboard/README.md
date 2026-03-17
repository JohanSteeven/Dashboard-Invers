# Dashboard de Admisiones Hospitalarias

Dashboard interactivo en Metabase que responde 5 preguntas de negocio sobre admisiones hospitalarias.


## Preguntas de Negocio

| # | Pregunta | Visualización |
|---|----------|---------------|
| Q1 | ¿Cuál es la tendencia de volumen de admisiones por mes? | Gráfico de líneas |
| Q2 | ¿Cuáles son los 10 hospitales con mayor facturación? | Barras horizontales |
| Q3 | ¿Cuál es la duración promedio de estancia por condición? | Barras agrupadas |
| Q4 | ¿Cómo varía la tasa de tests anormales por condición y aseguradora? | Tabla pivote |
| Q5 | ¿Existen diferencias en costo promedio entre aseguradoras? | Barras agrupadas |

---

## Composición del Dashboard

### KPIs (Fila Superior)

| KPI | Valor Esperado | Descripción |
|-----|----------------|-------------|
| Total Admisiones | ~54,966 | Volumen total de admisiones |
| Facturación Total | ~$1.40B | Facturación acumulada (excl. negativos) |
| Tasa de Anormales | ~33.6% | Proporción de tests anormales |
| Estancia Promedio | ~15.5 días | Duración promedio de estancia |

### Visualizaciones

| Tarjeta | Consulta SQL |
|---------|--------------|
| Admisiones Mensuales | `sql/02_dashboard_queries.sql` - Query Q1 |
| Top 10 Hospitales | `sql/02_dashboard_queries.sql` - Query Q2 |
| Facturación por Aseguradora | `sql/02_dashboard_queries.sql` - Query Q5 |
| Estancia por Condición | `sql/02_dashboard_queries.sql` - Query Q3 |
| Matriz de Anormalidad | `sql/02_dashboard_queries.sql` - Query Q4 |


