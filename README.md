# Tarea 3 — Pipeline de Streaming Avanzado con Apache Beam

Proyecto e implementación del pipeline de procesamiento de pagos en tiempo real para la asignatura **Streaming de datos y sus aplicaciones**.
El sistema procesa eventos de transacciones aplicando tiempo de evento, ventanas fijas, gestión de estado por clave (*stateful processing*), expiración por *timers* y materialización idempotente.

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
```text
docker compose up --build
docker exec -it streaming-fpuna-clase6-tarea-notebook-1 uv run pytest
```

## Evidencia

```text
Bytecode compiled 6639 files in 2.04s
======================================================= test session starts =======================================================
platform linux -- Python 3.12.12, pytest-8.4.2, pluggy-1.6.0
rootdir: /app
configfile: pyproject.toml
plugins: anyio-4.14.2, platformdirs-4.12.1
collected 13 items

tests/test_assignment.py .............                                                                                      [100%]

======================================================= 13 passed in 2.55s ========================================================
```

## Objetivos del Pipeline
Producir totales confirmados por comercio y por minuto cumpliendo con los siguientes requisitos:
* **Tiempo de Evento (```event_time```)**: Agrupación basada en el timestamp original del dominio y no en el tiempo de llegada (```arrival time```).
* **Ventanas Fijas (Fixed Windows):** Ventanas de 60 segundos con tolerancia de atraso (allowed lateness) de hasta 120 segundos.
* **Filtrado:** Descarte de eventos cuyo estado sea distinto de ```CONFIRMED```.
* **Deduplicación Stateful:** Eliminación de eventos duplicados (```event_id```) dentro del ámbito de cada comercio mediante estado por clave (```SetStateSpec```).
* **Expiración de Estado (Garbage Collection):** Limpieza explícita de la memoria del estado mediante un Timer (```TimerSpec```) configurado al vencimiento de la ventana + lateness.
* **Metadatos de Ventana:** Preservación de metadatos de inicio/fin de ventana y panes acumulativos para trazabilidad.
* **Salida Idempotente:** Generación de la clave determinística ```merchant_id|window_start``` por comercio y ventana para garantizar idempotencia en reintentos (UPSERT).

## Decisiones de Diseño y Trade-offs
**1. Manejo de Tiempo de Evento y Ventanas (Allowed Lateness)**
* **Decisión:** Se implementaron ventanas fijas (```FixedWindows```) de 60 segundos configurando ```.allowed_lateness=120``` y modo acumulativo (```ACCUMULATING```).
* **Trade-off:** Mantener el estado de la ventana abierto durante 120 segundos adicionales incrementa levemente el uso de memoria en el runner, pero permite procesar correctamente transacciones demoradas por latencia de red sin perder exactitud en los acumulados.

**2. Deduplicación por Clave y Expiración con Timer (```StateSpec + TimerSpec```)**
* **Decisión:** Para evitar un agrupamiento global de alta sobrecarga, la deduplicación de ```event_id``` se realiza mediante un ```DoFn``` con estado mutable por comercio (```SetStateSpec```). Se utiliza un timer de watermark (```TimerSpec```) a los 120s para borrar el estado cuando la ventana expira.
* **Trade-off:** Aislar por ```merchant_id``` restringe el estado al ámbito de cada comercio, mientras que el timer evita que la memoria crezca indefinidamente (memory leak) en escenarios de streaming continuo.

**3. Garantía de Idempotencia y Reintentos**
* **Decisión:** Cada registro emitido utiliza la clave compuesta determinística ```merchant_id|window_start (f"{merchant_id}|{window_start}")```.
* **Trade-off:** Requiere que el sumidero (sink) final soporte operaciones de tipo UPSERT (sobreescritura por clave primaria), asegurando que los reintentos o revisiones late actualicen el registro sin duplicar los totales en la base de datos final.
