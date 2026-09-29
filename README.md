# Tarea 3 — Pipeline de Streaming Avanzado con Apache Beam

Proyecto e implementación del pipeline de procesamiento de pagos en tiempo real para la asignatura **Streaming de datos y sus aplicaciones**.
El sistema procesa eventos de transacciones aplicando tiempo de evento, ventanas fijas, gestión de estado por clave (*stateful processing*) y materialización idempotente.

---

## Estructura del Proyecto

```text
.
├── data/
│   └── payments.jsonl          # Datos de entrada (eventos de pagos)
├── tests/                      # Suite de pruebas unitarias y de integración
├── notebook.py                 # Pipeline de Apache Beam e implementación (Marimo)
├── docker-compose.yml          # Entorno dockerizado para ejecución
├── pyproject.toml / uv.lock    # Configuración de dependencias con uv
└── README.md                   # Documentación y decisiones de arquitectura
```

## Instrucciones de Ejecución
docker exec -it streaming-fpuna-clase6-tarea-notebook-1 uv run pytest

## Evidencia
C:\Users\danny\streaming-fpuna-clase6-tarea>docker exec -it streaming-fpuna-clase6-tarea-notebook-1 uv run pytest
Bytecode compiled 6639 files in 2.04s
======================================================= test session starts =======================================================
platform linux -- Python 3.12.12, pytest-8.4.2, pluggy-1.6.0
rootdir: /app
configfile: pyproject.toml
plugins: anyio-4.14.2, platformdirs-4.12.1
collected 13 items

tests/test_assignment.py .............                                                                                      [100%]

======================================================= 13 passed in 2.55s ========================================================

What's next:
    Try Docker Debug for seamless, persistent debugging tools in any container or image → docker debug streaming-fpuna-clase6-tarea-notebook-1
    Learn more at https://docs.docker.com/go/debug-cli/

C:\Users\danny\streaming-fpuna-clase6-tarea>