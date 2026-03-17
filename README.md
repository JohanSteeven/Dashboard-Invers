# Healthcare Admissions Analytics — Data Engineering

Pipeline de datos completo para análisis de admisiones hospitalarias: desde exploración del dataset hasta dashboard interactivo en Metabase.

**Dataset:** [Healthcare Dataset](https://www.kaggle.com/datasets/prasad22/healthcare-dataset) — 55,500 registros de admisiones hospitalarias sintéticas.

---

## Resumen del Proyecto

| Aspecto | Descripción |
|---------|-------------|
| **Objetivo** | Transformar un dataset de admisiones hospitalarias en un modelo analítico consultable |
| **Alcance** | 4 fases obligatorias completadas + extensión opcional de automatización |
| **Tecnologías** | Python 3.12, PostgreSQL 16, Metabase, Docker |
| **Tiempo de ejecución** | ~5 minutos (fases 1-3) |

### Fases Implementadas

| Fase | Descripción | Artefactos |
|------|-------------|-----------|
| **1. Exploración** | Diagnóstico de calidad con 8 problemas priorizados | `reports/eda_report.md`, `diagnosis_report.json` |
| **2. Transformación** | 10 reglas de limpieza, 16 columnas derivadas | `healthcare_clean.csv`, `transformation_log.csv` |
| **3. Modelado** | Star schema (1 fact + 6 dims) en PostgreSQL | `01_schema.sql`, `load_report.md` |
| **4. Dashboard** | 10 tarjetas respondiendo 5 preguntas de negocio | Dashboard en Metabase |

---

## Arquitectura

```
  data/raw/                    reports/              PostgreSQL             Metabase
  healthcare_dataset.csv       *.md, *.csv           healthcare schema      Dashboard
        │                         ▲                       ▲                    ▲
        ▼                         │                       │                    │
  ┌──────────┐  reportes   ┌──────────┐  CSV limpio ┌──────────┐  vistas  ┌──────────┐
  │ Fase 1   │────────────▶│ Fase 2   │────────────▶│ Fase 3   │────────▶│ Fase 4   │
  │ Explorar │             │Transformar│            │  Cargar  │         │Dashboard │
  └──────────┘             └──────────┘             └──────────┘         └──────────┘
```

---

## Estructura del Repositorio

```
healthcare-analytics/
│
├── data/
│   ├── raw/healthcare_dataset.csv          # Dataset original (55,500 filas)
│   └── processed/healthcare_clean.csv      # Dataset limpio (54,966 filas)
│
├── src/
│   ├── phase1_explore.py                   # Fase 1: EDA y diagnóstico
│   ├── phase2_transform.py                 # Fase 2: Limpieza y transformación
│   └── phase3_load.py                      # Fase 3: Carga a PostgreSQL
│
├── sql/
│   ├── 01_schema.sql                       # DDL del star schema
│   └── 02_dashboard_queries.sql            # Queries del dashboard
│
├── reports/                                # Artefactos generados
│   ├── eda_report.md, diagnosis_report.json
│   ├── transformation_report.md, transformation_log.csv
│   ├── load_report.md, data_model_description.md
│   
│
├── dashboard/
│   ├── metabase_setup.md                   # Guía de construcción del dashboard
│   └── findings_narrative.md               # Narrativa de hallazgos
│
├── docs/
│   ├── data_analysis.md                    # Diagnóstico técnico completo
│   └── data_model.md                       # Modelo dimensional
│
├── optional/                               # Extensión opcional (Fase 5)
│   ├── README.md
│   ├── pipeline_api.py
│   └── n8n_workflow.json
│
├── docker-compose.yml                      # PostgreSQL + Metabase + n8n + Pipeline API
├── requirements.txt
├── .env.example
└── README.md
```

---

## Ejecución Rápida

### Prerequisitos

- Python 3.10+
- Docker Desktop 20.10+
- Docker Compose v2+

### Paso 1: Clonar y Configurar

```bash
git clone https://github.com/JohanSteeven/Dashboard-Invers
cd Dashboard-Invers

# Entorno Python
python -m venv .venv
source .venv/bin/activate    # Linux/Mac
# .venv\Scripts\activate     # Windows
pip install -r requirements.txt

# Variables de entorno
cp .env.example .env
```

### Paso 2: Levantar Infraestructura

```bash
docker compose up -d
# Esperar ~30 segundos para que los servicios estén listos
docker compose ps
```

Esto levanta 4 servicios:

| Servicio | URL | Descripción |
|----------|-----|-------------|
| PostgreSQL | `localhost:5432` | Base de datos analítica |
| Metabase | `http://localhost:3000` | Dashboard (Fase 4) |
| Pipeline API | `http://localhost:8080` | API HTTP para automatización |
| n8n | `http://localhost:5678` | Orquestador visual (Fase 5) |

### Paso 3: Ejecutar Pipeline (Fases 1-3)

```bash
python src/phase1_explore.py      # ~60s — genera reportes EDA
python src/phase2_transform.py    # ~90s — genera CSV limpio
python src/phase3_load.py         # ~120s — carga a PostgreSQL
```

### Paso 4: Configurar Dashboard (Fase 4)

1. Abrir `http://localhost:3000`
2. Crear cuenta admin en Metabase
3. Conectar a PostgreSQL:
   - Host: `healthcare-postgres`
   - Puerto: `5432`
   - Base de datos: `healthcare_db`
   - Usuario: `postgres`
   - Contraseña: `admin`
4. Seguir `dashboard/metabase_setup.md` para crear las 10 tarjetas

### Verificar Resultados

```bash
# Confirmar datos cargados
docker exec healthcare-postgres psql -U postgres -d healthcare_db \
  -c "SELECT COUNT(*) FROM healthcare.fact_admissions"
# Resultado esperado: 54966
```

---

## Preguntas de Negocio

El dashboard responde 5 preguntas analíticas:

| # | Pregunta | Visualización |
|---|----------|---------------|
| Q1 | ¿Cuál es la tendencia de volumen de admisiones por mes? | Línea temporal |
| Q2 | ¿Cuáles son los 10 hospitales con mayor facturación? | Barras horizontales |
| Q3 | ¿Cuál es la duración promedio de estancia por condición? | Barras agrupadas |
| Q4 | ¿Cómo varía la tasa de test anormales por condición y aseguradora? | Tabla pivote |
| Q5 | ¿Existen diferencias en costo promedio entre aseguradoras? | Barras agrupadas |

---

## Resultados Principales

| Métrica | Valor |
|---------|-------|
| Registros originales | 55,500 |
| Duplicados eliminados | 534 |
| Registros finales | 54,966 |
| Columnas derivadas | 16 |
| Reglas de limpieza | 10 |
| Validaciones aprobadas | 22 |
| Tablas dimensión | 6 |
| Vistas analíticas | 5 |
| Tarjetas dashboard | 10 |

---

## Decisiones Técnicas

### Modelo de Datos
**Star schema** elegido sobre 3NF porque:
- Consultas del dashboard son agregaciones por dimensiones categóricas
- Minimiza JOINs y simplifica consultas en Metabase
- Dimensiones de baja cardinalidad (3-6 valores)

### Reglas de Limpieza
- Duplicados exactos eliminados (534 filas)
- Facturación negativa y baja marcadas con indicadores (no eliminadas)
- Cuasi-duplicados conservados (posibles readmisiones legítimas)
- Normalización de texto con Title Case

### Reproducibilidad
- Scripts ejecutables en secuencia sin estado externo
- Carga idempotente con `ON CONFLICT DO NOTHING`
- Infraestructura containerizada con Docker Compose

---

## Limitaciones Conocidas

- **Dataset sintético:** Distribuciones uniformes impiden análisis estadísticos significativos
- **Sin identificador de paciente:** No es posible analizar readmisiones
- **Alta cardinalidad en hospitales:** ~40,000 hospitales únicos con ~1.4 admisiones promedio

---

## Fase 5: Automatización con n8n

El proyecto incluye una fase adicional que automatiza la ejecución del pipeline mediante n8n y una API HTTP mínima. Los servicios se levantan automáticamente con `docker compose up -d`.

| Servicio | URL | Credenciales |
|----------|-----|--------------|
| n8n (Orquestador) | `http://localhost:5678` | admin / admin123 |
| Pipeline API | `http://localhost:8080` | — |

**Para usar la automatización:**

1. Abrir n8n en `http://localhost:5678`
2. Importar el workflow desde `optional/n8n_workflow.json`
3. Hacer clic en "Execute Workflow" para ejecutar las fases 1-3 automáticamente

> **Nota:** La creación del dashboard (Fase 4) no está incluida en la automatización porque Metabase requiere configuración manual inicial (crear cuenta admin y conectar la base de datos) antes de poder usar su API. Una vez configurado, el dashboard se construye siguiendo `dashboard/README.md`.

Ver `optional/README.md` para documentación completa.

---

## Detener Servicios

```bash
docker compose down           # Detener sin eliminar datos
docker compose down --volumes # Detener y eliminar volúmenes
```

---

## Tecnologías

| Tecnología | Versión | Propósito |
|------------|---------|-----------|
| Python | 3.12 | ETL, perfilado de datos, carga |
| PostgreSQL | 16 | Modelo dimensional, vistas analíticas |
| Metabase | Latest | Dashboard interactivo |
| Docker Compose | v2 | Orquestación de contenedores |

---

## Autor

Desarrollado como prueba técnica de Data Engineering.

**Dataset:** [prasad22/healthcare-dataset](https://www.kaggle.com/datasets/prasad22/healthcare-dataset) (Kaggle)
