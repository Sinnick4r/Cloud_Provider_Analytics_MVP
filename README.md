
# Cloud Provider Analytics - MVP Tecnico

## Descripción del Proyecto

Este repositorio contiene la implementacion tecnica de un pipeline de datos "end-to-end" basado en una Arquitectura Lambda para el análisis de proveedores de nube. El sistema ingesta logs de uso de recursos (streaming) y datos maestros de facturacion/CRM (batch) para generar métricas de FinOps (costos, uso y huella de carbono).

El proyecto maneja el flujo de datos a traves de las capas del Data Lake (Landing, Bronze, Silver, Gold) utilizando PySpark y finaliza en una capa de Serving alojada en AstraDB (Cassandra) para consumo de baja latencia.


## Herramientas

*   **Procesamiento:** Apache Spark 3.x (PySpark) sobre Google Colab.
*   **Arquitectura:** Lambda (Batch + Speed Layers).
*   **Almacenamiento:** Parquet con particionamiento Hive-style (simulacion local).
*   **Streaming:** Spark Structured Streaming con gestion de estado (watermarks).
*   **Serving Layer:** DataStax AstraDB (Cassandra) con conector nativo.
*   **Lenguaje:** Python 3.10+.

## Estructura del Repositorio

En el codigo se separan la config, los esquemas y la lgica de negocio por capas.

```text
.
+-- config.py                   # Configuración global, Paths y SparkSession
+-- schemas.py                  # Definiciones de esquemas (StructType) explícitos
+-- io_utils.py                 # Funciones wrapper para lectura/escritura (I/O)
+-- audit.py                    # Scripts de auditoría y validacion de calidad
¦
+-- bronze_batch.py             # Ingesta Batch: CSV -> Parquet (con chequeo)
+-- bronze_stream.py            # Ingesta Streaming: JSONL -> Parquet (con dedupe)
+-- silver.py                   # Joins, DQ y logica de Cuarentena
+-- gold.py                     # Agregacion: Data Mart de FinOps (Daily Grain)
¦
+-- cassandra_utils.py          # Gestion de conexión segura a AstraDB
+-- cassandra_loader.py         # Carga de datos Gold a Cassandra
+-- cassandra_queries.py        # Consultas de negocio (CQL) para validacion
¦
+-- notebooks/                  # Notebooks de ejecucio (.ipynb)
+-- cred.env.example            # Plantilla de variables de entorno
+-- README.md                   # Documentación del proyecto
```

## Detalle de los Pipelines

### 1. Ingesta y Bronze Layer
*   **Batch:** Procesa archivos CSV maestros (Customers, Users, Billing). Se estandarizan tipos de datos y se añaden columnas de trazabilidad (`ingest_ts`, `source_file`).
*   **Streaming:** Consume eventos JSONL desde `landing/usage_events_stream`. Implementa `dropDuplicates` por `event_id` y `withWatermark` para manejar datos tardios y garantizar una ingesta "at-least-once" robusta.

### 2. Silver Layer (Calidad y Enriquecimiento)
Realiza el join entre los eventos de uso y la dimensión de organizaciones. Se aplican reglas de calidad (Quality Gates):
*   Validacion de integridad referencial (`org_id`).
*   Validacion de reglas de negocio (ej. `cost_usd_increment >= -0.01`).
*   **Cuarentena:** Los registros invalidos se desvian a una tabla de cuarentena. .

### 3. Gold Layer (FinOps Mart)
Genera el dataset agregado `org_daily_usage_by_service`.
*   **categorias:** Organizacion, Servicio, Fecha.
*   **metricas:** Costo total, Peticiones, Huella de Carbono, Costo por Peticion.

### 4. Serving Layer (Cassandra)
Carga los datos agregados a AstraDB utilizando el driver de Python. La tabla `org_daily_usage_by_service` esta modelada bajo un enfoque "Query-First", utilizando `org_id` como Partition Key para optimizar las consultas de costos por cliente.

## Idempotencia y Manejo de Fallos

El sistema garantiza consistencia ante re-ejecuciones mediante:
1.  **Batch:** Escritura en modo `overwrite` (o partición dinámica) en capas Bronze/Silver.
2.  **Streaming:** Uso de directorios de Checkpoint para recuperación de estado.
3.  **Cassandra:** Uso de `UPSERT` natural basado en la Primary Key compuesta.
4.  **Cuarentena:** Se usa un left anti join contra el histórico de errores para evitar duplicados en reprocesos.

## Instrucciones de Ejecucion

1.  Clonar el repositorio.
2.  Configurar el archivo `cred.env` con el Token de AstraDB y ubicar el `secure-connect-bundle.zip` en la raíz.
3.  Asegurar la presencia del dataset `cloud_provider_challenge_dataset_v1.zip` en la carpeta `data/`.
4.  Ejecutar el notebook principal en `notebooks/` o los scripts de python en orden secuencial (Bronze -> Silver -> Gold -> Loader).