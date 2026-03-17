# Fase 5: Automatización con n8n

Esta carpeta contiene los componentes de la **Fase 5** que automatiza la ejecución del pipeline ETL mediante n8n y una API HTTP mínima.

> **Nota:** Esta fase es una extensión que demuestra capacidades de automatización y orquestación. Los servicios se levantan automáticamente con `docker compose up -d`.

---

## Componentes

| Archivo | Descripción |
|---------|-------------|
| `pipeline_api.py` | Servidor HTTP mínimo que expone los scripts ETL |
| `n8n_workflow.json` | Workflow n8n exportable e importable |

---

## Arquitectura

```
┌─────────────────┐     HTTP GET      ┌─────────────────┐
│                 │ ─────────────────▶│                 │
│   n8n           │     /phase1       │  pipeline-api   │
│   (Orquestador) │     /phase2       │  (Python HTTP)  │
│                 │     /phase3       │                 │
│   :5678         │     /run-all      │  :8080          │
└─────────────────┘◀─────────────────┘└─────────────────┘
                        Respuesta JSON            │
                                               │ subprocess
                                               ▼
                    ┌──────────────────────────────────────┐
                    │  phase1_explore.py                   │
                    │  phase2_transform.py                 │
                    │  phase3_load.py                      │
                    └──────────────────────────────────────┘
```

---

## Pipeline API

### Endpoints

| Método | Endpoint | Descripción |
|--------|----------|-------------|
| GET | `/health` | Verificación de disponibilidad |
| GET | `/phase1` | Ejecuta Fase 1: Exploración EDA |
| GET | `/phase2` | Ejecuta Fase 2: Limpieza y transformación |
| GET | `/phase3` | Ejecuta Fase 3: Carga a PostgreSQL |
| GET | `/run-all` | Ejecuta las 3 fases en secuencia |

### Respuesta de Éxito

```json
{
  "status": "success",
  "phases_executed": ["phase1", "phase2", "phase3"],
  "results": [
    {
      "phase": "phase1",
      "script": "phase1_explore.py",
      "returncode": 0,
      "status": "success"
    }
  ]
}
```

### Respuesta de Error

```json
{
  "error": "Fase /phase2 falló (código 1)",
  "results": [
    {
      "phase": "phase1",
      "status": "success"
    },
    {
      "phase": "phase2",
      "status": "error",
      "stderr_tail": "..."
    }
  ]
}
```

---

## Workflow n8n

El archivo `n8n_workflow.json` contiene un workflow completo con los siguientes nodos:

| Nodo | Tipo | Función |
|------|------|---------|
| Manual Trigger | Trigger | Inicia el pipeline manualmente |
| Verificar API | HTTP Request | GET /health |
| Fase 1 — Exploración EDA | HTTP Request | GET /phase1 (timeout: 5min) |
| Fase 2 — Limpieza y Transformación | HTTP Request | GET /phase2 (timeout: 5min) |
| Fase 3 — Carga a PostgreSQL | HTTP Request | GET /phase3 (timeout: 10min) |
| Resumen de Ejecución | Code | Verifica resultados y genera resumen |

### Flujo de Ejecución

```
Manual Trigger → Verificar API → Fase 1 → Fase 2 → Fase 3 → Resumen
```

---

## Uso

### Paso 1: Levantar Servicios

Todos los servicios (incluyendo n8n y pipeline-api) se levantan con:

```bash
docker compose up -d
```

Esto levanta 4 contenedores:
- `healthcare-postgres` — PostgreSQL en `localhost:5432`
- `healthcare-metabase` — Metabase en `http://localhost:3000`
- `healthcare-pipeline` — Pipeline API en `http://localhost:8080`
- `healthcare-n8n` — n8n en `http://localhost:5678`

### Paso 2: Verificar Servicios

```bash
# Verificar que todos los contenedores estén corriendo
docker compose ps

# Probar el health endpoint
curl http://localhost:8080/health
```

### Paso 3: Importar Workflow en n8n

1. Abrir `http://localhost:5678`
2. Credenciales: `admin` / `admin123`
3. Configuración → Importar desde archivo
4. Seleccionar `optional/n8n_workflow.json`
5. Hacer clic en "Import"

### Paso 4: Ejecutar el Pipeline

1. Abrir el workflow importado
2. Hacer clic en "Execute Workflow"
3. Observar la ejecución nodo por nodo
4. Verificar el resumen final

---

## Variables de Entorno

Las siguientes variables se configuran en `.env`:

```env
# n8n (Fase 5)
N8N_USER=admin
N8N_PASSWORD=admin123
```

La API del pipeline usa las mismas variables de PostgreSQL que los scripts principales:

```env
DB_HOST=postgres        # Nombre del contenedor Docker
DB_PORT=5432
DB_NAME=healthcare_db
DB_USER=postgres
DB_PASSWORD=admin
```

---

## Limitaciones

- **Sin programación automática:** El trigger es manual. Puede agregarse un Schedule Trigger en n8n.
- **Sin alertas por email:** Puede agregarse un nodo de Email en n8n tras el Resumen.
- **Sin reintento automático:** Si una fase falla, el pipeline se detiene. Puede agregarse lógica de reintento en n8n.

---

## Archivos Relacionados

| Ubicación | Descripción |
|-----------|-------------|
| `../src/phase1_explore.py` | Script de Fase 1 |
| `../src/phase2_transform.py` | Script de Fase 2 |
| `../src/phase3_load.py` | Script de Fase 3 |
| `../docker-compose.yml` | Todos los servicios |
