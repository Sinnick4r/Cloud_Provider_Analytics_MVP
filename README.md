
# Cloud Provider Analytics - MVP Tecnico

## Descripci�n del Proyecto

Este repositorio contiene la implementacion tecnica de un pipeline de datos "end-to-end" basado en una Arquitectura Lambda para el an�lisis de proveedores de nube. El sistema ingesta logs de uso de recursos (streaming) y datos maestros de facturacion/CRM (batch) para generar m�tricas de FinOps (costos, uso y huella de carbono).

El proyecto maneja el flujo de datos a traves de las capas del Data Lake (Landing, Bronze, Silver, Gold) utilizando PySpark y finaliza en una capa de Serving alojada en AstraDB (Cassandra) para consumo de baja latencia.


## Herramientas

*   **Procesamiento:** PySpark sobre Google Colab.
*   **Arquitectura:** Lambda (Batch + Speed Layers).
*   **Almacenamiento:** Parquet con particionamiento Hive-style (simulacion local).
*   **Streaming:** Spark Structured Streaming con gestion de estado (watermarks).
*   **Serving Layer:** DataStax AstraDB (Cassandra) con conector nativo.
*   **Lenguaje:** Python 3.10+.

## Estructura del Repositorio

En el codigo se separan la config, los esquemas y la lgica de negocio por capas.
Los archivos .py están en proceso, ahora se entrega el notebook con el codigo. En la entrega final se dividiran las celdas
sus correspondintes .py



```text
.
    src/               # Código (ETL)
│   ├── config.py      # Configuraciones globales y SparkSession
│   ├── schemas.py     # Esquemas explicitos y estaticos
│   ├── io_utils.py    # Lectura/Escritura gengrica
│   ├── quality_utils.py # Auditoria y Data Observability
│   ├── bronze_*.py    # Ingesta Batch y Streaming
│   ├── silver.py      # Etapa silver - limpieza y enriquecimiento
│   ├── gold.py        # Estapa Gold - agregación y Speed Layer
│   ├── cassandra_utils.py # Conector nativo a AstraDB
│   ├── cassandra_loader.py         # Carga de datos Gold a Cassandra
│   └── cassandra_queries.py        # Consultas de negocio (CQL) para validacion
├── notebook/                   # Notebooks de ejecucio (.ipynb) 
├── data/                       # (Ignorado en git) Espacio para el dataset            
├── cred-ejemplo.env.txt        # Plantilla de variables de entorno
├── README.md                   # Documentaci�n del proyecto
└── DECISION_LOG.md             # Log de decisiones
```

## Detalle de los Pipelines

### 1. Ingesta y Bronze Layer
*   **Batch:** Procesa archivos CSV maestros (Customers, Users, Billing). Se estandarizan tipos de datos y se a�aden columnas de trazabilidad (`ingest_ts`, `source_file`).
*   **Streaming:** Consume eventos JSONL desde `landing/usage_events_stream`. Implementa `dropDuplicates` por `event_id` y `withWatermark` para manejar datos tardios y garantizar una ingesta "at-least-once" robusta.

### 2. Silver Layer (Calidad y Enriquecimiento)
Realiza el join entre los eventos de uso y la dimensi�n de organizaciones. Se aplican reglas de calidad (Quality Gates):
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
1.  **Batch:** Escritura en modo `overwrite` (o partici�n din�mica) en capas Bronze/Silver.
2.  **Streaming:** Uso de directorios de Checkpoint para recuperaci�n de estado.
3.  **Cassandra:** Uso de `UPSERT` natural basado en la Primary Key compuesta.
4.  **Cuarentena:** Se usa un left anti join contra el hist�rico de errores para evitar duplicados en reprocesos.

## Instrucciones de Ejecucion

1.  Clonar el repositorio.
2.  Configurar el archivo `cred.env` con el Token de AstraDB y ponerlo junto con el `secure-connect-bundle.zip`en la carpeta `creds/`.
3.  El formato correcto del contenido de `cred.env` esta en el archivo cred-ejemplo.env.txt del Repo
4.  Asegurar la presencia del dataset `cloud_provider_challenge_dataset_v1.zip` en la carpeta `data/`.
5.  Ejecutar el notebook principal en `notebooks/` o los scripts de python en orden secuencial (Bronze -> Silver -> Gold -> Loader).