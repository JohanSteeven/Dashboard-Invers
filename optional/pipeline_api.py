"""
Pipeline API — Servidor HTTP mínimo que expone los scripts ETL como endpoints.

Usado por n8n (Fase 5) a través del nodo HTTP Request para orquestar
el pipeline completo desde un workflow n8n sin dependencias externas.

Endpoints:
  GET /health          — verificación de disponibilidad
  GET /phase1          — Exploración y diagnóstico (phase1_explore.py)
  GET /phase2          — Limpieza y transformación (phase2_transform.py)
  GET /phase3          — Modelado y carga a PostgreSQL (phase3_load.py)
  GET /run-all         — Ejecuta las 3 fases en secuencia

Uso:
  python src/pipeline_api.py [puerto]   # por defecto: 8080
"""

import json
import subprocess
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

PHASE_SCRIPTS = {
    "/phase1": PROJECT_ROOT / "src" / "phase1_explore.py",
    "/phase2": PROJECT_ROOT / "src" / "phase2_transform.py",
    "/phase3": PROJECT_ROOT / "src" / "phase3_load.py",
}

RUN_ALL_ORDER = ["/phase1", "/phase2", "/phase3"]


class PipelineHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = self.path.split("?")[0]  # ignorar query strings

        if path == "/health":
            self._respond(200, {"status": "ok", "service": "healthcare-pipeline-api"})

        elif path in PHASE_SCRIPTS:
            self._run_phases([path])

        elif path == "/run-all":
            self._run_phases(RUN_ALL_ORDER)

        else:
            self._respond(404, {"error": f"Endpoint '{path}' no encontrado",
                                "endpoints": ["/health", "/phase1", "/phase2",
                                              "/phase3", "/run-all"]})

    def _run_phases(self, phase_keys: list):
        results = []
        for key in phase_keys:
            script = PHASE_SCRIPTS[key]
            print(f"[pipeline-api] Ejecutando {script.name}...", flush=True)
            try:
                proc = subprocess.run(
                    [sys.executable, str(script)],
                    capture_output=True,
                    text=True,
                    timeout=600,
                    cwd=str(PROJECT_ROOT),
                )
                entry = {
                    "phase": key.lstrip("/"),
                    "script": script.name,
                    "returncode": proc.returncode,
                    "stdout_tail": proc.stdout[-3000:] if proc.stdout else "",
                    "stderr_tail": proc.stderr[-1000:] if proc.stderr else "",
                    "status": "success" if proc.returncode == 0 else "error",
                }
                results.append(entry)
                if proc.returncode != 0:
                    self._respond(500, {
                        "error": f"Fase {key} falló (código {proc.returncode})",
                        "results": results,
                    })
                    return
            except subprocess.TimeoutExpired:
                self._respond(504, {"error": f"Timeout en {key}", "results": results})
                return
            except Exception as exc:
                self._respond(500, {"error": str(exc), "results": results})
                return

        self._respond(200, {
            "status": "success",
            "phases_executed": [r["phase"] for r in results],
            "results": results,
        })

    def _respond(self, code: int, body: dict):
        data = json.dumps(body, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, fmt, *args):
        # Formato de log limpio en lugar del Apache-style por defecto
        print(f"[pipeline-api] {self.address_string()} - {fmt % args}", flush=True)


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
    server = HTTPServer(("0.0.0.0", port), PipelineHandler)
    print(f"[pipeline-api] Servidor iniciado en http://0.0.0.0:{port}", flush=True)
    print(f"[pipeline-api] Endpoints: /health /phase1 /phase2 /phase3 /run-all", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[pipeline-api] Detenido.", flush=True)
