# Proyecto “Cloud Provider Analytics” – Final

  

>  **Formato:** Documento oficial de entrega del MVP final.

  

---

  


-  **Título del proyecto:** Cloud Provider Analytics: Plataforma de Inteligencia de Costos y Operaciones

-  **Integrante:**  Emilio Gomez Lencina

  
  

## Resumen Ejecutivo

  
Este proyecto  implementa un ecosistema analítico para un proveedor de nube ficticio que necesita integrar datos de **FinOps, Soporte y Producto/GenAI** a partir de fuentes heterogéneas y con evolución de esquema (`schema_version`, aparición de `carbon_kg` y `genai_tokens`) para habilitar analítica diaria y _near real-time_.

Se adopta un arquitectura de solución tipo **Lambda**: pipeline batch para maestros (CRM, facturación, NPS) y pipeline de _Structured Streaming_ para eventos de uso (`usage_events_stream`) sobre un data lake Parquet (Landing → Bronze → Silver → Gold), con publicación final de _marts_ en **AstraDB (Cassandra)** para consumo por herramientas de BI.

El MVP final implementa: 
1.  ingesta batch de CSV a Bronze con esquemas tipados; 
2.  _streaming_ de eventos JSONL con `withWatermark` + `dropDuplicates(event_id)`;
3.  limpieza y conformidad en Silver, incluyendo integración con dimensiones de organizaciones y recursos; 
4. _marts_ Gold para FinOps (`org_daily_usage_by_service`), Soporte (`org_daily_support_metrics`) y GenAI (`org_daily_genai_usage`); 
5.  carga idempotente a Cassandra con driver Python y pruebas explícitas de reproceso; 
6.  consultas de negocio desde AstraDB 

El resultado es un pipeline *end-to-end* que alimenta una *Serving Layer* en AstraDB (Cassandra), permitiendo consultas de alto rendimiento. Los usuarios pueden obtener *insights* inmediatos, como identificar los servicios más costosos de la última semana o correlacionar picos de incidentes críticos con fechas de despliegue, todo validado mediante un dashboard interactivo.


---

  

## Arquitectura Final

  
**1. Diagrama de alto nivel**

El flujo implementa una arquitectura Lambda clásica. Los datos CSV (Maestros) siguen la ruta Batch (Landing $\rightarrow$ Bronze $\rightarrow$ Silver $\rightarrow$ Gold), mientras que los eventos JSON siguen la ruta Speed (Streaming $\rightarrow$ Gold Directo). Ambas rutas convergen en la capa de servicio (Cassandra).

  

![Diagrama de flujo de arquitectura Lambda](https://github.com/Sinnick4r/Cloud_Provider_Analytics_MVP/blob/main/documentacion/Diagrama%20Proyecto.png?raw=true)

  

Se eligió un patrón de **Arquitectura Lambda** para combinar la robustez del procesamiento *Batch* (necesario para la conciliación precisa de facturación histórica y datos maestros) con la baja latencia del *Speed Layer* (crítico para alertas de consumo en tiempo real), permitiendo responder preguntas de negocio complejas sobre costos y operaciones.


**2. Patrón elegido y Decisiones clave**

*  **Particionamiento:**  `hq_region` para maestros estáticos y `event_date` para eventos transaccionales, optimizando la lectura.

*  **Formatos:** Parquet en todo el Data Lake por eficiencia de I/O y soporte de esquemas.

  
**3. Componentes y dependencias**

*  **Procesamiento:** Apache Spark 3.5.1 (PySpark).

*  **Storage:** Sistema de archivos local simulando S3/HDFS.

*  **Serving:** DataStax AstraDB (Cassandra) + Driver Python.

*  **Dependencias:**  `pyspark`, `cassandra-driver`, `astrapy`, `gradio`, `plotly`.

  ---

  

## Datos y Supuestos

  
Los datos provienen de un entorno simulado de proveedor de nube. Se normalizaron timestamps a UTC y montos a decimales. A continuación, el diccionario de datos completo basado en los esquemas definidos en `schemas.py`.

  

**Diccionario de datos clave (extracto):**

  

| Campo | Dataset | Tipo | Descripción | Observaciones |
|---|---|---|---|---|
| `org_id` | customers_orgs | String | ID único de la organización | Clave primaria y de partición en Cassandra. |
| `hq_region` | customers_orgs | String | Región de la sede | Usado para particionamiento en Bronze. |
| `plan_tier` | customers_orgs | String | Nivel de plan (Free, Pro, Enterprise) | Usado para segmentación en Silver. |
| `user_id` | users | String | ID único del usuario | - |
| `role` | users | String | Rol del usuario (Admin, Viewer) | - |
| `resource_id` | resources | String | ID del recurso en la nube | - |
| `service` | resources | String | Nombre del servicio (compute, storage) | - |
| `ticket_id` | support_tickets | String | ID del ticket de soporte | - |
| `severity` | support_tickets | String | Criticidad (Low, Medium, Critical) | Usado para métricas de SLA. |
| `sla_breached` | support_tickets | Boolean | Flag de incumplimiento de SLA | True si se venció el plazo. |
| `invoice_id` | billing_monthly | String | ID de la factura mensual | - |
| `subtotal` | billing_monthly | Decimal | Monto antes de impuestos | - |
| `event_id` | usage_events | String | ID único del evento de uso | Clave para deduplicación. |
| `cost_usd_increment` | usage_events | Decimal | Costo incremental del evento | Regla de calidad: $\ge -0.01$. |
| `carbon_kg` | usage_events | Double | Huella de carbono estimada | Métrica ESG. |
| `genai_tokens` | usage_events | Long | Tokens consumidos (si aplica) | Métrica específica GenAI (derivada). |

  
---

## Data Lake: Zonas y Particionado

  
El Data Lake se organiza estrictamente en zonas para garantizar la gobernanza y calidad del dato:

*  **Landing:** Datos crudos inmutables (CSV/JSON).

*  **Bronze:** Raw estándar, mismo grano que la fuente, tipificación explícita, dedupe por `event_id` y columnas técnicas `ingest_ts`, `source_file`

*  **Silver:** datos limpios y conformados con joins a dimensiones y tratamiento de nulos/outliers . Particionado por `event_date`.

*  **Gold:** Marts de negocio agregados. Particionado por `event_date` o `ticket_date`.

*  **Quarantine:** Zona aislada para registros que no cumplen reglas de calidad (ej. costos negativos).
  

![Diagrama de Pipelines de Datos](https://github.com/Sinnick4r/Cloud_Provider_Analytics_MVP/blob/main/documentacion/Pipelines.png?raw=true)

###  Directorios del datalake
  ![Diagrama de Pipelines de Datos](https://github.com/Sinnick4r/Cloud_Provider_Analytics_MVP/blob/main/documentacion/estructura%20de%20directorios.png?raw=true)

---

  

## Ingesta y Calidad de Datos

  

### Ingesta Batch

Se implementaron funciones dedicadas para cada dataset maestro, aplicando lectura con esquema estricto, adición de columnas de auditoría y escritura particionada en formato Parquet.

  

*Fragmento de código (`bronze_batch.py`):*

```python

def  ingest_customers_orgs_to_bronze(spark: SparkSession) -> None:

df = _read_landing_csv(spark, "customers_orgs.csv", customers_orgs_schema)

if df is  None: return

df = add_audit_columns(df)

dest = zone_path(BRONZE_PATH, "customers_orgs")

write_parquet(df, dest, partition_cols=["hq_region"])

  

def  ingest_users_to_bronze(spark: SparkSession) -> None:

df = _read_landing_csv(spark, "users.csv", users_schema)

if df is  None: return

df = add_audit_columns(df)

dest = zone_path(BRONZE_PATH, "users")

write_parquet(df, dest, partition_cols=["role"])

  

def  ingest_resources_to_bronze(spark: SparkSession) -> None:

df = _read_landing_csv(spark, "resources.csv", resources_schema)

if df is  None: return

df = add_audit_columns(df)

dest = zone_path(BRONZE_PATH, "resources")

write_parquet(df, dest, partition_cols=["region"])

  

def  ingest_support_tickets_to_bronze(spark: SparkSession) -> None:

df = _read_landing_csv(spark, "support_tickets.csv", support_tickets_schema)

if df is  None: return

df = add_audit_columns(df)

dest = zone_path(BRONZE_PATH, "support_tickets")

write_parquet(df, dest, partition_cols=["severity"])

  

def  ingest_marketing_touches_to_bronze(spark: SparkSession) -> None:

df = _read_landing_csv(spark, "marketing_touches.csv", marketing_touches_schema)

if df is  None: return

df = add_audit_columns(df)

dest = zone_path(BRONZE_PATH, "marketing_touches")

write_parquet(df, dest, partition_cols=["channel"])

  

def  ingest_nps_surveys_to_bronze(spark: SparkSession) -> None:

df = _read_landing_csv(spark, "nps_surveys.csv", nps_surveys_schema)

if df is  None: return

df = add_audit_columns(df)

dest = zone_path(BRONZE_PATH, "nps_surveys")

write_parquet(df, dest, partition_cols=["survey_date"])

  

def  ingest_billing_monthly_to_bronze(spark: SparkSession) -> None:

df = _read_landing_csv(spark, "billing_monthly.csv", billing_monthly_schema)

if df is  None: return

df = add_audit_columns(df)

dest = zone_path(BRONZE_PATH, "billing_monthly")

write_parquet(df, dest, partition_cols=["month"])

```

  

### Ingesta Streaming

Lectura de JSONs en streaming con transformación de fechas, aplicación de *watermarking* de 1 día y deduplicación por `event_id` para garantizar integridad.

  

*Fragmento de código (`bronze_stream.py`):*

```python

def  start_usage_events_to_bronze(spark: SparkSession):

df_stream = create_usage_events_stream(spark)

# Transformaciones: timestamp -> event_ts, watermark, dedupe

df_bronze = transform_usage_events_bronze(df_stream)

  

dest_path = zone_path(BRONZE_PATH, "usage_events")

checkpoint_path = BRONZE_PATH / "_checkpoints" / "usage_events"

  

query = (

df_bronze

.writeStream

.format("parquet")

.option("checkpointLocation", str(checkpoint_path))

.option("path", str(dest_path))

.partitionBy("event_date")

.outputMode("append")

.start()

)

return query
```
### Resultado de la ingesta Streaming:

```python console
[INFO] Comenzando Speed Layer...
[INFO] Dimensión Organizaciones cacheada: 80 registros.
[INFO] Streaming ejecutandose -> /content/datalake/gold/org_daily_usage_by_service_speed
Streaming Speed → Gold
ID: a3016673-1db2-49d6-a7f8-27d726d023e7
Nombre: None
Activo: True
[STREAM 0] Reporte 
 Input: 360 eventos
Validos: 359
Dropped (Cost < -0.01): 1 (0.3%)
todo pasado a Gold de Speed layer
[STREAM 1] Reporte 
 Input: 360 eventos
Validos: 358
Dropped (Cost < -0.01): 2 (0.6%)
todo pasado a Gold de Speed layer
[STREAM 2] Reporte 
 Input: 11 eventos
Validos: 11
todo pasado a Gold de Speed layer
[STREAM 3] Reporte 
 Input: 7 eventos
Validos: 7
todo pasado a Gold de Speed layer
 Progreso del streaming
{'id': 'a3016673-1db2-49d6-a7f8-27d726d023e7', 'runId': '7f48b7be-d5b7-47dc-b5c2-63ed1b19bef1', 'name': None, 'timestamp': '2025-12-07T19:21:25.000Z', 'batchId': 3, 'numInputRows': 360, 'inputRowsPerSecond': 150.9433962264151, 'processedRowsPerSecond': 173.49397590361446, 'durationMs': {'addBatch': 1863, 'commitOffsets': 56, 'getBatch': 11, 'latestOffset': 54, 'queryPlanning': 32, 'triggerExecution': 2075, 'walCommit': 54}, 'eventTime': {'avg': '2025-08-02T03:04:24.833Z', 'max': '2025-08-31T23:51:00.000Z', 'min': '2025-07-03T00:48:00.000Z', 'watermark': '2025-08-30T23:51:00.000Z'}, 'stateOperators': [{'operatorName': 'dedupe', 'numRowsTotal': 2410, 'numRowsUpdated': 25, 'allUpdatesTimeMs': 194, 'numRowsRemoved': 0, 'allRemovalsTimeMs': 0, 'commitTimeMs': 969, 'memoryUsedBytes': 481296, 'numRowsDroppedByWatermark': 1142, 'numShufflePartitions': 13, 'numStateStoreInstances': 13, 'customMetrics': {'loadedMapCacheHitCount': 74, 'loadedMapCacheMissCount': 40, 'numDroppedDuplicateRows': 0, 'stateOnCurrentVersionSizeBytes': 445792}}], 'sources': [{'description': 'FileStreamSource[file:/content/datalake/landing/usage_events_stream]', 'startOffset': {'logOffset': 2}, 'endOffset': {'logOffset': 3}, 'latestOffset': None, 'numInputRows': 360, 'inputRowsPerSecond': 150.9433962264151, 'processedRowsPerSecond': 173.49397590361446}], 'sink': {'description': 'ForeachBatchSink', 'numOutputRows': -1}}
aparando stream...

 cheque de Speed Layer ---
Ruta: /content/datalake/gold/org_daily_usage_by_service_speed, Total acumulado en disco: 727
funcionando todo OK (Datos persistidos correctamente)
+------------+---------------+------------+--------------+--------------+---------------+----------+
|org_id      |org_name       |service_name|daily_cost_usd|daily_requests|daily_carbon_kg|event_date|
+------------+---------------+------------+--------------+--------------+---------------+----------+
|org_pnsm43d8|Delta Labs 70  |compute     |11.6645       |130.0000      |0.026          |2025-08-13|
|org_dhylurtp|Nimbus Cloud 76|compute     |11.9243       |133.0000      |0.0266         |2025-08-13|
|org_5iqvnb4g|Gamma Labs 73  |networking  |0.0092        |0.0000        |2.05E-4        |2025-08-13|
+------------+---------------+------------+--------------+--------------+---------------+----------+
only showing top 3 rows

Streaming Speed → Gold parado.
```

  

### 5.3 Reglas de Calidad y Evidencias

Se verifica unicidad de claves primarias y completitud de campos de auditoría. En una unica celda se chequeo la integridad de todo lo realizado en las capas Bronze, Silver y Gold.

*Salida de ejecución (`audit.py`):*

```python console

[2025-12-07 19:20:51] [RUN] Haciendo quality control...  [2025-12-07 19:20:51] [RUN] Iniciando Auditoría completa de capa Bronze... chequeo Bronze: customers_orgs
registros totales: 80
duplicados en PK (org_id): 0
nulos en ingest_ts: 0
Resultado: todo ok!!

 chequeo Bronze: users
registros totales: 800
duplicados en PK (user_id): 0
nulos en ingest_ts: 0
Resultado: todo ok!!

 chequeo Bronze: resources
registros totales: 400
duplicados en PK (resource_id): 0
nulos en ingest_ts: 0
Resultado: todo ok!!

 chequeo Bronze: support_tickets
registros totales: 1000
duplicados en PK (ticket_id): 0
nulos en ingest_ts: 0
Resultado: todo ok!!

 chequeo Bronze: marketing_touches
registros totales: 1500
duplicados en PK (touch_id): 0
nulos en ingest_ts: 0
Resultado: todo ok!!

 chequeo Bronze: billing_monthly
registros totales: 240
duplicados en PK (invoice_id): 0
nulos en ingest_ts: 0
Resultado: todo ok!!

 chequeo Bronze: nps_surveys
registros totales: 92
duplicados en PK (org_id): 32
nulos en ingest_ts: 0
Resultado: revisar data por posibles duplicados [2025-12-07 19:20:58] [OK] Auditoría Bronze finalizada.  [2025-12-07 19:20:58] [RUN] Iniciando Auditoría de Calidad Silver... Chequeop Silver:
Total: 758
Aceptados (Silver): 755
Rechazados (cuarentena): 3 (0.40%)
CALIDAD: ACEPTABLE
ejemplo de rechazo:
+----------------+-------------------------+
|event_id        |quarantine_reason        |
+----------------+-------------------------+
|evt_faubjbtabmwl|cost_negative_or_null_org|
+----------------+-------------------------+
only showing top 1 row


Calidad de Datos (Silver Batch)

 Estadisticas:
Total Procesado: 758
Aceptados (Silver): 755 (99.60%)
Rechazados (Quarantine): 3 (0.40%)

resultado:
Aceptable- rechazo  bajo y esperado.

 Muestra de registros en Cuarentena (top 5):
+----------------+------------------+------------+-------------------------+
|event_id        |cost_usd_increment|org_id      |quarantine_reason        |
+----------------+------------------+------------+-------------------------+
|evt_faubjbtabmwl|-0.0602           |org_cvs4f8cg|cost_negative_or_null_org|
|evt_qniow8ymxwd6|-0.2446           |org_i7p5tb94|cost_negative_or_null_org|
|evt_bbmth9hzpa6e|-0.3656           |org_n9j2qp89|cost_negative_or_null_org|
+----------------+------------------+------------+-------------------------+ [2025-12-07 19:21:06] [OK] Auditoría Silver finalizada.  [2025-12-07 19:21:06] [RUN] Iniciando Auditoría de Marts Gold... Chequeo Gold: org_daily_usage_by_service
registros Totales (Agregados): 727
Costos Negativos detectados: 0

 Chequeo Gold: org_daily_support_metrics
registros Totales (Agregados): 944
Costos Negativos detectados: 0

 Chequeo Gold: org_daily_genai_usage
registros Totales (Agregados): 77
Costos Negativos detectados: 0 [2025-12-07 19:21:11] [OK] Auditoría Gold finalizada.  [2025-12-07 19:21:11] [OK] Fin demo Batch Layer (Bronze CSV → Silver → Gold)

```  

---

  

## Transformaciones ( Etapa Silver)

  

En la capa Silver (`run_silver_batch`), se realizan transformaciones críticas de limpieza y enriquecimiento. Se utiliza un **Broadcast Join** para cruzar los eventos masivos con los datos de organizaciones.

  

**Cálculos realizados:**

*  **Suma de Costos:** Agregación de `cost_usd_increment` como `daily_cost_usd`.

*  **Conteo de Requests:** Suma condicional donde `metric == 'requests'`.

*  **Cálculo de Carbono:** Acumulación directa de `carbon_kg`.

*  **KPIs Derivados:**  `cost_per_request` y `carbon_per_dollar`.

*  **Soporte:** Conteo de `sla_breached` para obtener la tasa de incumplimiento (`sla_breach_rate`).

  

*Fragmento de código (Reglas de Calidad y Cuarentena):*

```python

# Reglas: Costo >= -0.01 y org_id no nulo

dq_condition = (F.col("cost_usd_increment") >= -0.01) & (F.col("org_id").isNotNull())

good_df = enriched_df.filter(dq_condition)

bad_df = enriched_df.filter(~dq_condition)

  

# Logica Left Anti para evitar duplicados en cuarentena

existing_quarantine = read_parquet(spark, quarantine_dest)

unique_bad_df = bad_df.join(existing_quarantine, on="event_id", how="left_anti")

```

  ### Resultado de la ingesta + Silver a gold (celda demo)
```python console
[OK] Dataset descomprimido en /content/datalake/landing
[2025-12-07 19:19:40] [INFO] PROJECT_ROOT: {PROJECT_ROOT}  [2025-12-07 19:19:40] [INFO] Spark version: 3.5.1  [2025-12-07 19:19:40] [RUN] Generando Bronze Batch (Maestros)... [OK] Dataset descomprimido en /content/datalake/landing

[BATCH] Iniciando Ingesta a Bronze (7 Maestros)... 

Procesando Archivos: 100%

 7/7 [00:19<00:00,  1.83s/tablas]

[INFO] Leyendo /content/datalake/landing/customers_orgs.csv
[OK] Bronze customers_orgs -> /content/datalake/bronze/customers_orgs
[INFO] Leyendo /content/datalake/landing/users.csv
[OK] Bronze users -> /content/datalake/bronze/users
[INFO] Leyendo /content/datalake/landing/resources.csv
[OK] Bronze resources -> /content/datalake/bronze/resources
[INFO] Leyendo /content/datalake/landing/support_tickets.csv
[OK] Bronze support_tickets -> /content/datalake/bronze/support_tickets
[INFO] Leyendo /content/datalake/landing/marketing_touches.csv
[OK] Bronze marketing_touches -> /content/datalake/bronze/marketing_touches
[INFO] Leyendo /content/datalake/landing/nps_surveys.csv
[OK] Bronze nps_surveys -> /content/datalake/bronze/nps_surveys
[INFO] Leyendo /content/datalake/landing/billing_monthly.csv
[OK] Bronze billing_monthly -> /content/datalake/bronze/billing_monthly
[2025-12-07 19:20:03] [OK] Capa Bronze finalizada correctamente  [2025-12-07 19:20:03] [RUN] Generando Bronze Stream (Eventos)... [INFO] Streaming usage_events -> /content/datalake/bronze/usage_events
[INFO] Checkpoints en /content/datalake/bronze/_checkpoints/usage_events [2025-12-07 19:20:19] [OK] Stream procesado.  [2025-12-07 19:20:19] [RUN] Generando Silver... [INFO] Iniciando Silver...
[WARN] Creando cuarentena por primera vez
[OK] Silver Batch completado, todo ok -> /content/datalake/silver/usage_events_enriched [2025-12-07 19:20:38] [OK] Silver finalizado  [2025-12-07 19:20:38] [RUN] ejecuntando proceso Gold... [OK] Gold Batch (FinOps) -> /content/datalake/gold/org_daily_usage_by_service
[OK] Gold Batch (Support) -> /content/datalake/gold/org_daily_support_metrics
[OK] Gold Batch (GenAI) -> /content/datalake/gold/org_daily_genai_usage [2025-12-07 19:20:51] [OK] proceso Gold finalizado
```


---

  

## Modelado Gold y Serving en AstraDB (Cassandra)

  

### Diseño por consulta (query-first)

Se diseñaron tablas con claves compuestas (`org_id` como Partition Key) para satisfacer consultas de dashboards por cliente y rango de fechas.  

El modelo de datos se diseñó siguiendo la metodología *Query-First* de Cassandra, optimizando las tablas para los filtros específicos de los dashboards de negocio.

  

**1. Caso de Uso: FinOps (Tabla `org_daily_usage_by_service`)**

*  **Partition Key:**  `org_id`.

*  *Justificación:* Los dashboards siempre filtran por un cliente específico. Esto asegura que todos los datos de una organización residan en el mismo nodo, maximizando la velocidad de lectura.

*  **Clustering Keys:**  `usage_date` (DESC), `service_name` (ASC).

*  *Justificación:* El ordenamiento por fecha descendente permite obtener rápidamente los datos "últimos 30 días" sin ordenar en memoria. El servicio asegura la unicidad del registro diario.

*  **TTL:** No aplica. Se requiere retención histórica indefinida para auditorías de facturación y comparativas anuales (Year-over-Year).

  

**2. Caso de Uso: Soporte (Tabla `org_daily_support_metrics`)**

*  **Partition Key:**  `org_id`.

*  **Clustering Key:**  `ticket_date` (DESC).

*  *Justificación:* Permite consultas de rango eficientes para un cliente dado.

  

**3. Caso de Uso: Producto/GenAI (Tabla `org_daily_genai_usage`)**

*  **Partition Key:**  `org_id`.

*  **Clustering Keys:**  `event_date` (DESC), `service_name`.

*  *Justificación:* Segrega el tráfico de alto volumen de GenAI para no afectar la lectura de costos generales.
  

### Scripts CQL

A continuación se incluyen todas las *queries* CQL implementadas en `cassandra_utils.py` para la creación de las tablas de FinOps, Soporte y GenAI.


```sql

-- 1. Tabla FinOps: Costos y métricas operativas diarias por servicio

CREATE  TABLE  IF  NOT  EXISTS  "Cloud_analytics_db".org_daily_usage_by_service (

org_id text,

usage_date date,

service_name  text,

daily_cost_usd double,

daily_requests double,

daily_carbon_kg double,

cost_per_request double,

carbon_per_dollar double,

PRIMARY KEY ((org_id), usage_date, service_name)

) WITH CLUSTERING ORDER BY (usage_date DESC, service_name  ASC);

  
-- 2. Tabla de Soporte: Métricas de tickets, SLAs y satisfacción

CREATE  TABLE  IF  NOT  EXISTS  "Cloud_analytics_db".org_daily_support_metrics (

org_id text,

ticket_date date,

total_tickets int,

critical_tickets int,

sla_breached_count int,

avg_csat double,

sla_breach_rate double,

PRIMARY KEY ((org_id), ticket_date)

) WITH CLUSTERING ORDER BY (ticket_date DESC);

-- 3. Tabla de GenAI: Consumo específico de tokens y costos de IA

CREATE  TABLE  IF  NOT  EXISTS  "Cloud_analytics_db".org_daily_genai_usage (

org_id text,

event_date date,

service_name  text,

genai_daily_cost double,

genai_requests_count double,

PRIMARY KEY ((org_id), event_date, service_name)

) WITH CLUSTERING ORDER BY (event_date DESC);
 
```

### Carga a Cassandra

*Evidencia de carga a Cassandra (`cassandra_loader.py` output):*

```python console
[SERVING] Iniciando carga a AstraDB...
[CASSANDRA] Creando esquema en keyspace 'Cloud_analytics_db'...
[CASSANDRA] Esquema verificado: 3 Tablas listas.
   -> cargando tabla 'org_daily_usage_by_service' (731 registros)...

Subiendo org_daily_usage_by_service: 100%

 731/731 [00:15<00:00, 46.48rows/s]

      [OK] Carga completada.
   -> cargando tabla 'org_daily_support_metrics' (944 registros)...

Subiendo org_daily_support_metrics: 100%

 944/944 [00:20<00:00, 46.19rows/s]

      [OK] Carga completada.
   -> cargando tabla 'org_daily_genai_usage' (78 registros)...

Subiendo org_daily_genai_usage: 100%

 78/78 [00:01<00:00, 47.25rows/s]

      [OK] Carga completada.
```

### Chequeo de tablas

```python console
chequeo de carga en Cassandra (Keyspace: Cloud_analytics_db)

Tabla: `org_daily_usage_by_service`
- Total de registros : 876

Muestra de datos:

| org_id       | usage_date   | service_name   |   carbon_per_dollar |   cost_per_request |   daily_carbon_kg |   daily_cost_usd |   daily_requests |
|:-------------|:-------------|:---------------|--------------------:|-------------------:|------------------:|-----------------:|-----------------:|
| org_c11ertj5 | 2025-08-31   | compute        |          0.00326907 |          0.0638383 |          0.025043 |           7.6606 |              120 |
| org_c11ertj5 | 2025-08-31   | database       |          0.00413361 |          0.0504168 |          0.0248   |           5.9996 |              119 |
| org_c11ertj5 | 2025-08-31   | genai          |          0.00157653 |          0.127047  |          0.049072 |          31.1266 |              245 |
| org_c11ertj5 | 2025-08-18   | compute        |          0.00292469 |        nan         |          0.000167 |           0.0571 |                0 |
| org_c11ertj5 | 2025-08-13   | database       |          0.00418779 |        nan         |          0.000446 |           0.1065 |                0 |

---

Tabla: `org_daily_support_metrics`
- Total de registros : 944

Muestra de datos:

| org_id       | ticket_date   |   avg_csat |   critical_tickets |   sla_breach_rate |   sla_breached_count |   total_tickets |
|:-------------|:--------------|-----------:|-------------------:|------------------:|---------------------:|----------------:|
| org_c11ertj5 | 2025-08-30    |          3 |                  0 |                 0 |                    0 |               1 |
| org_c11ertj5 | 2025-08-16    |          3 |                  0 |                 0 |                    0 |               1 |
| org_c11ertj5 | 2025-08-07    |          5 |                  0 |                 1 |                    1 |               1 |
| org_c11ertj5 | 2025-08-03    |          2 |                  0 |                 0 |                    0 |               1 |
| org_c11ertj5 | 2025-07-23    |        nan |                  0 |                 0 |                    0 |               1 |

---

Tabla: `org_daily_genai_usage`
- Total de registros : 92

Muestra de datos:

| org_id       | event_date   | service_name   |   genai_daily_cost |   genai_requests_count |
|:-------------|:-------------|:---------------|-------------------:|-----------------------:|
| org_c11ertj5 | 2025-08-31   | genai          |            31.1266 |                    245 |
| org_c11ertj5 | 2025-08-07   | genai          |             0.2865 |                      0 |
| org_c11ertj5 | 2025-07-21   | genai          |            12.6785 |                    110 |
| org_w3zp08j3 | 2025-08-31   | genai          |             0.3548 |                      0 |
| org_kdgigatj | 2025-07-15   | genai          |             0      |                      0 |

---
```

  

---
  

## Idempotencia y Reprocesos

  

La idempotencia se garantiza mediante el diseño de claves primarias naturales en Cassandra, lo que convierte las inserciones en *Upserts*. Se validó ejecutando el proceso de carga dos veces consecutivas y verificando que los conteos no aumentaran.

  *Evidencia (Salida del chequeo de Idempotencia):*

```python console

Prueba de idempotencia 

[1] Contando registros actuales...
    - org_daily_usage_by_service: 876
    - org_daily_support_metrics: 944
    - org_daily_genai_usage: 92

[2] >>> Re-ejecutando Carga (Gold -> Cassandra)...

[SERVING] Iniciando carga masiva a AstraDB...
[CASSANDRA] Creando esquema en keyspace 'Cloud_analytics_db'...
[CASSANDRA] Esquema verificado: 3 Tablas listas.
   -> cargando tabla 'org_daily_usage_by_service' (727 registros)...

Subiendo org_daily_usage_by_service: 100%

 727/727 [00:26<00:00, 27.93rows/s]

      [OK] Carga completada.
   -> cargando tabla 'org_daily_support_metrics' (944 registros)...

Subiendo org_daily_support_metrics: 100%

 944/944 [00:34<00:00, 28.07rows/s]

      [OK] Carga completada.
   -> cargando tabla 'org_daily_genai_usage' (77 registros)...

Subiendo org_daily_genai_usage: 100%

 77/77 [00:02<00:00, 27.60rows/s]

      [OK] Carga completada.

[3] Verificando consistencia...
    - org_daily_usage_by_service: 876 -> 876 [OK]
    - org_daily_support_metrics: 944 -> 944 [OK]
    - org_daily_genai_usage: 92 -> 92 [OK]
--------------------------------------------------
Resultado: 0 Duplicados generados.
```
Como se puede ver, al realizar upserts los duplicados terminan siendo cero, confirmando la idempotencia en la base de Cassandra

---


## Performance

Para garantizar un rendimiento óptimo en un entorno de recursos limitados (Single-Node en Colab) y minimizar la latencia, se implementaron tres estrategias clave de optimización en Spark:

1.  **Broadcast Hash Join (Map-Side Join):** En la capa Silver, al cruzar la tabla de hechos masiva (`usage_events`) con la dimensión pequeña (`customers_orgs`), se forzo un `F.broadcast()`. Esto evita el intercambio de datos (*Shuffle*) a través de la red, enviando una copia completa de la tabla de clientes a la memoria de cada ejecutor, transformando un *SortMergeJoin* en un *BroadcastHashJoin* mucho más rápido.
2.  **Ajuste de Shuffle Partitions:** Se configuró `spark.sql.shuffle.partitions = 4` (el default es 200). Dado que el volumen de datos del MVP es moderado (inferior a 10 GB), mantener 200 particiones generaría una sobrecarga excesiva en la planificación de tareas (*task scheduling overhead*) y fragmentación de archivos pequeños (*small files*). Al reducirlo a 4, se optimizan las tareas, maximizando el output
3.  **Caching en Streaming:** En la *Speed Layer*, se aplicó `.cache()` a la dimensión de organizaciones antes de iniciar el flujo. Sin esto, Spark volvería a leer los archivos Parquet de la capa Bronze en cada *micro-batch* (cada 5 segundos), degradando la latencia I/O. Al mantener la tabla en memoria RAM, el *lookup* es instantáneo.

**Métricas observadas:**
Gracias a estas optimizaciones, el procesamiento de la capa Silver (limpieza y joins) mantuvoun  tiempo de ejecución de **~12 segundos**. Asimismo, la carga a Cassandra mantuvo una tasa de escritura constante de **~45 filas/segundo**, limitada únicamente por el ancho de banda de red hacia AstraDB y no por el procesamiento de Spark.
 
 ---
  
## Consultas mínimas desde AstraDB

Se armaron las siguientes Queries CQL para realizar uan evaluacion del estado de las tablas subidas a Cassandra yver que tan factible es realizar consultas de negocios reales.
Si bien se pedian 5 queries, se realizó una sexta respecto al rema sustentabilidad para asi mostrar mas  posibiliades de las tablas.

 **Queries**

```python
# (1) FinOps: Costos y requests diarios (rango de fechas)
    print("\n1. [FinOps] Costos y requests diarios (Últimos 7 días)")
    query_1 = f"""
        SELECT usage_date, service_name, daily_cost_usd, daily_requests
        FROM "{KEYSPACE}".org_daily_usage_by_service
        WHERE org_id = '{TARGET_ORG}'
        AND usage_date >= '{ref_date - timedelta(days=7)}'
    """
    rows = session.execute(query_1)
    print(f"{'Fecha':<12} | {'Servicio':<15} | {'Costo ($)':<10} | {'Requests'}")
    for r in rows:
        print(f"{str(r.usage_date):<12} | {r.service_name:<15} | {r.daily_cost_usd:<10.2f} | {int(r.daily_requests)}")


    # (2) FinOps: Top 3 servicios mas costosos (ultimos 14 dias)
    print(f"\n2. [FinOps] Top 3 Servicios más costosos (Desde {date_14_days_ago})")
    query_2 = f"""
        SELECT service_name, daily_cost_usd
        FROM "{KEYSPACE}".org_daily_usage_by_service
        WHERE org_id = '{TARGET_ORG}'
        AND usage_date >= '{date_14_days_ago}'
    """
    rows = session.execute(query_2)

    # agregacion client-side
    cost_map = {}
    for r in rows:
        cost_map[r.service_name] = cost_map.get(r.service_name, 0.0) + r.daily_cost_usd

    sorted_services = sorted(cost_map.items(), key=lambda x: x[1], reverse=True)[:3]

    print(f"{'Servicio':<15} | {'Costo Acum. ($)'}")
    for svc, cost in sorted_services:
        print(f"{svc:<15} | {cost:.2f}")


    # (3) Soporte: Tickets criticos y SLA breach rate (30 dias)
    print(f"\n3. [Soporte] Tickets críticos y Tasa SLA Breach (Desde {date_30_days_ago})")
    query_3 = f"""
        SELECT ticket_date, critical_tickets, sla_breach_rate
        FROM "{KEYSPACE}".org_daily_support_metrics
        WHERE org_id = '{TARGET_ORG}'
        AND ticket_date >= '{date_30_days_ago}'
    """
    rows = session.execute(query_3)

    # manejo por si no hay tickets
    if not rows:
        print("(Sin tickets en este periodo para esta org)")

    print(f"{'Fecha':<12} | {'Críticos':<8} | {'Tasa Breach %'}")
    for r in rows:
        rate_pct = r.sla_breach_rate * 100 if r.sla_breach_rate else 0.0
        print(f"{str(r.ticket_date):<12} | {r.critical_tickets:<8} | {rate_pct:.1f}%")


    # (4) Finanzas: Revenue mensual estimado (con tax)
    # nota: tomo desde usage
    print(f"\n4. [Finanzas] Revenue Mensual Estimado (Mes actual: {start_of_month})")

    query_4 = f"""
        SELECT daily_cost_usd
        FROM "{KEYSPACE}".org_daily_usage_by_service
        WHERE org_id = '{TARGET_ORG}'
        AND usage_date >= '{start_of_month}'
    """
    rows = session.execute(query_4)

    monthly_subtotal = sum([r.daily_cost_usd for r in rows])
    taxes = monthly_subtotal * 0.21 # 21% tax -tome el IVA como ejemplo, eso lo decide finanzas
    credits = 0.0
    total_revenue = monthly_subtotal + taxes - credits

    print(f"Subtotal Uso:    ${monthly_subtotal:.2f}")
    print(f"Impuestos (est): ${taxes:.2f}")
    print(f"TOTAL REVENUE:   ${total_revenue:.2f}")


    # (5) Producto: Consumo GenAI (tokens y costo)
    print("\n5. [Producto] Consumo GenAI (Tokens/Requests y Costo)")
    query_5 = f"""
        SELECT event_date, service_name, genai_requests_count, genai_daily_cost
        FROM "{KEYSPACE}".org_daily_genai_usage
        WHERE org_id = '{TARGET_ORG}'
        LIMIT 5
    """
    rows = list(session.execute(query_5))

    if not rows:
        print("(Esta organización no registra consumo de GenAI)")
    else:
        print(f"{'Fecha':<12} | {'Servicio':<12} | {'Tokens/Reqs':<12} | {'Costo ($)'}")
        for r in rows:
            print(f"{str(r.event_date):<12} | {r.service_name:<12} | {int(r.genai_requests_count):<12} | {r.genai_daily_cost:.2f}")

  # (6) Sustentabilidad
    print("\n6. [ESG] Huella de Carbono")
    query_4 = f"""
        SELECT usage_date, service_name, daily_carbon_kg
        FROM "{KEYSPACE}".org_daily_usage_by_service
        WHERE org_id = '{TARGET_ORG}'
        LIMIT 5
    """
    rows = session.execute(query_4)
    print(f"   {'Fecha':<12} | {'Servicio':<15} | {'Carbono (kgCO2)'}")
    print("   " + "-"*50)
    for r in rows:
        print(f"   {str(r.usage_date):<12} | {r.service_name:<15} | {f'{r.daily_carbon_kg:.4f}' if r.daily_carbon_kg is not None else 'None':<15}")
```
### Resultados de las queries - output de celda  `cassandra_queries.py`:

_Observación_ : la celda toma una organizacion random y genera las tablas.
```python console
Queries de negocios 

Organizacion: org_pja1wj0t
Fecha de Referencia: 2025-08-31
------------------------------------------------------------

1. [FinOps] Costos y requests diarios (Últimos 7 días)
Fecha        | Servicio        | Costo ($)  | Requests
2025-08-31   | compute         | 11.93      | 111
2025-08-31   | networking      | 2.51       | 234
2025-08-27   | compute         | 0.11       | 0

2. [FinOps] Top 3 Servicios más costosos (Desde 2025-08-17)
Servicio        | Costo Acum. ($)
compute         | 12.04
networking      | 2.51

3. [Soporte] Tickets críticos y Tasa SLA Breach (Desde 2025-08-01)
Fecha        | Críticos | Tasa Breach %
2025-08-23   | 0        | 100.0%
2025-08-22   | 0        | 0.0%
2025-08-20   | 0        | 0.0%

4. [Finanzas] Revenue Mensual Estimado (Mes actual: 2025-08-01)
Subtotal Uso:    $14.56
Impuestos (est): $3.06
TOTAL REVENUE:   $17.61

5. [Producto] Consumo GenAI (Tokens/Requests y Costo)
(Esta organización no registra consumo de GenAI)

6. [ESG] Huella de Carbono
   Fecha        | Servicio        | Carbono (kgCO2)
   --------------------------------------------------
   2025-08-31   | compute         | 0.0255         
   2025-08-31   | networking      | 0.0468         
   2025-08-27   | compute         | 0.0002         
   2025-08-05   | networking      | 0.0001         
   2025-07-24   | compute         | 0.0005

```


## Limitaciones principales:

-   El cálculo de Revenue se realiza en tiempo de consulta (On-the-fly) en lugar de pre-agregarse en un Mart físico, lo cual es aceptable para el volumen actual pero no escala bien
    
-   La detección de anomalías se reduce a un umbral simple sobre `cost_usd_increment` y a flags implícitos de calidad; no se utilizaron los tres métodos estadísticos sugeridos (z-score, MAD, percentiles) a los fines de simplificar la entrega.
    
-   La **gestión de `schema_version`** se limita a conservar la columna y explotar `carbon_kg` cuando existe; no hay lógica diferenciada v1/v2 ni backfill explícito para campos nuevos.
    
-  El pipeline tolera cambios de esquema (columna `schema_version`), pero no implementa lógica de backfilling automático ni tablas SCD Tipo 2 para cambios en dimensiones lentas (Organizaciones/Recursos).
    
-   La métricas de GenAI agregan `requests` como proxy de tokens; el agregado explícito de `genai_tokens` queda pendiente

-   Debido a las restricciones del entorno de Google Colab, la orquestación de tareas Batch y Streaming se simuló secuencialmente en un notebook. Un entorno productivo requeriría Cron/Apache Airflow para gestión de dependencias y reintentos.



## Trabajo futuro propuesto:

-   Implementar  un Hardening de Calidad en la capa Silver para incluir validaciones estadísticas (desviación estándar de costos) antes de la carga a Gold. en la capa Silver para incluir validaciones estadísticas (desviación estándar de costos) antes de la carga a Gold.

- Materializar la tabla de Revenue con contadores distribuidos para evitar el costo de agregación en lectura.

- Incorporar z-score, MAD y percentiles para flags de anomalía y consolidarlos en un mart consumible por FinOps.

- Implementar lógica específica por `schema_version` (v1/v2) y, si fuera necesario, tablas SCD para organizaciones/recursos.    

-   Migrar la ejecución a un orquestador (Airflow/cron + Prometheus/Grafana) y conectar directamente AstraDB a Power BI/Looker para dashboards productivos.

- Definir políticas de Backup y Disaster Recovery para el Data Lake y AstraDB. 

- Implementar cifrado en reposo/tránsito y controles de acceso (RBAC) para garantizar el cumplimiento de normativas (GDPR/SOC2) antes del despliegue productivo.# Proyecto “Cloud Provider Analytics” – Final

  

>  **Formato:** Documento oficial de entrega del MVP final.

  

---

  


-  **Título del proyecto:** Cloud Provider Analytics: Plataforma de Inteligencia de Costos y Operaciones

-  **Integrantes:** [Completar con Nombres]

  
  

## Resumen Ejecutivo

  
Este proyecto  implementa un ecosistema analítico para un proveedor de nube ficticio que necesita integrar datos de **FinOps, Soporte y Producto/GenAI** a partir de fuentes heterogéneas y con evolución de esquema (`schema_version`, aparición de `carbon_kg` y `genai_tokens`) para habilitar analítica diaria y _near real-time_.

Se adopta un arquitectura de solución tipo **Lambda**: pipeline batch para maestros (CRM, facturación, NPS) y pipeline de _Structured Streaming_ para eventos de uso (`usage_events_stream`) sobre un data lake Parquet (Landing → Bronze → Silver → Gold), con publicación final de _marts_ en **AstraDB (Cassandra)** para consumo por herramientas de BI.

El MVP final implementa: 
1.  ingesta batch de CSV a Bronze con esquemas tipados; 
2.  _streaming_ de eventos JSONL con `withWatermark` + `dropDuplicates(event_id)`;
3.  limpieza y conformidad en Silver, incluyendo integración con dimensiones de organizaciones y recursos; 
4. _marts_ Gold para FinOps (`org_daily_usage_by_service`), Soporte (`org_daily_support_metrics`) y GenAI (`org_daily_genai_usage`); 
5.  carga idempotente a Cassandra con driver Python y pruebas explícitas de reproceso; 
6.  consultas de negocio desde AstraDB 

El resultado es un pipeline *end-to-end* que alimenta una *Serving Layer* en AstraDB (Cassandra), permitiendo consultas de alto rendimiento. Los usuarios pueden obtener *insights* inmediatos, como identificar los servicios más costosos de la última semana o correlacionar picos de incidentes críticos con fechas de despliegue, todo validado mediante un dashboard interactivo.


---

  

## Arquitectura Final

  
**1. Diagrama de alto nivel**

El flujo implementa una arquitectura Lambda clásica. Los datos CSV (Maestros) siguen la ruta Batch (Landing $\rightarrow$ Bronze $\rightarrow$ Silver $\rightarrow$ Gold), mientras que los eventos JSON siguen la ruta Speed (Streaming $\rightarrow$ Gold Directo). Ambas rutas convergen en la capa de servicio (Cassandra).

  

![Diagrama de flujo de arquitectura Lambda](https://github.com/Sinnick4r/Cloud_Provider_Analytics_MVP/blob/main/documentacion/Diagrama%20Proyecto.png?raw=true)

  

Se eligió un patrón de **Arquitectura Lambda** para combinar la robustez del procesamiento *Batch* (necesario para la conciliación precisa de facturación histórica y datos maestros) con la baja latencia del *Speed Layer* (crítico para alertas de consumo en tiempo real), permitiendo responder preguntas de negocio complejas sobre costos y operaciones.


**2. Patrón elegido y Decisiones clave**

*  **Particionamiento:**  `hq_region` para maestros estáticos y `event_date` para eventos transaccionales, optimizando la lectura.

*  **Formatos:** Parquet en todo el Data Lake por eficiencia de I/O y soporte de esquemas.

  
**3. Componentes y dependencias**

*  **Procesamiento:** Apache Spark 3.5.1 (PySpark).

*  **Storage:** Sistema de archivos local simulando S3/HDFS.

*  **Serving:** DataStax AstraDB (Cassandra) + Driver Python.

*  **Dependencias:**  `pyspark`, `cassandra-driver`, `astrapy`, `gradio`, `plotly`.

  ---

  

## Datos y Supuestos

  
Los datos provienen de un entorno simulado de proveedor de nube. Se normalizaron timestamps a UTC y montos a decimales. A continuación, el diccionario de datos completo basado en los esquemas definidos en `schemas.py`.

  

**Diccionario de datos clave (extracto):**

  

| Campo | Dataset | Tipo | Descripción | Observaciones |
|---|---|---|---|---|
| `org_id` | customers_orgs | String | ID único de la organización | Clave primaria y de partición en Cassandra. |
| `hq_region` | customers_orgs | String | Región de la sede | Usado para particionamiento en Bronze. |
| `plan_tier` | customers_orgs | String | Nivel de plan (Free, Pro, Enterprise) | Usado para segmentación en Silver. |
| `user_id` | users | String | ID único del usuario | - |
| `role` | users | String | Rol del usuario (Admin, Viewer) | - |
| `resource_id` | resources | String | ID del recurso en la nube | - |
| `service` | resources | String | Nombre del servicio (compute, storage) | - |
| `ticket_id` | support_tickets | String | ID del ticket de soporte | - |
| `severity` | support_tickets | String | Criticidad (Low, Medium, Critical) | Usado para métricas de SLA. |
| `sla_breached` | support_tickets | Boolean | Flag de incumplimiento de SLA | True si se venció el plazo. |
| `invoice_id` | billing_monthly | String | ID de la factura mensual | - |
| `subtotal` | billing_monthly | Decimal | Monto antes de impuestos | - |
| `event_id` | usage_events | String | ID único del evento de uso | Clave para deduplicación. |
| `cost_usd_increment` | usage_events | Decimal | Costo incremental del evento | Regla de calidad: $\ge -0.01$. |
| `carbon_kg` | usage_events | Double | Huella de carbono estimada | Métrica ESG. |
| `genai_tokens` | usage_events | Long | Tokens consumidos (si aplica) | Métrica específica GenAI (derivada). |

  
---

## Data Lake: Zonas y Particionado

  
El Data Lake se organiza estrictamente en zonas para garantizar la gobernanza y calidad del dato:

*  **Landing:** Datos crudos inmutables (CSV/JSON).

*  **Bronze:** Raw estándar, mismo grano que la fuente, tipificación explícita, dedupe por `event_id` y columnas técnicas `ingest_ts`, `source_file`

*  **Silver:** datos limpios y conformados con joins a dimensiones y tratamiento de nulos/outliers . Particionado por `event_date`.

*  **Gold:** Marts de negocio agregados. Particionado por `event_date` o `ticket_date`.

*  **Quarantine:** Zona aislada para registros que no cumplen reglas de calidad (ej. costos negativos).
  

![Diagrama de Pipelines de Datos](https://github.com/Sinnick4r/Cloud_Provider_Analytics_MVP/blob/main/documentacion/Pipelines.png?raw=true)

###  Directorios del datalake
  ![Diagrama de Pipelines de Datos](https://github.com/Sinnick4r/Cloud_Provider_Analytics_MVP/blob/main/documentacion/estructura%20de%20directorios.png?raw=true)

---

  

## Ingesta y Calidad de Datos

  

### Ingesta Batch

Se implementaron funciones dedicadas para cada dataset maestro, aplicando lectura con esquema estricto, adición de columnas de auditoría y escritura particionada en formato Parquet.

  

*Fragmento de código (`bronze_batch.py`):*

```python

def  ingest_customers_orgs_to_bronze(spark: SparkSession) -> None:

df = _read_landing_csv(spark, "customers_orgs.csv", customers_orgs_schema)

if df is  None: return

df = add_audit_columns(df)

dest = zone_path(BRONZE_PATH, "customers_orgs")

write_parquet(df, dest, partition_cols=["hq_region"])

  

def  ingest_users_to_bronze(spark: SparkSession) -> None:

df = _read_landing_csv(spark, "users.csv", users_schema)

if df is  None: return

df = add_audit_columns(df)

dest = zone_path(BRONZE_PATH, "users")

write_parquet(df, dest, partition_cols=["role"])

  

def  ingest_resources_to_bronze(spark: SparkSession) -> None:

df = _read_landing_csv(spark, "resources.csv", resources_schema)

if df is  None: return

df = add_audit_columns(df)

dest = zone_path(BRONZE_PATH, "resources")

write_parquet(df, dest, partition_cols=["region"])

  

def  ingest_support_tickets_to_bronze(spark: SparkSession) -> None:

df = _read_landing_csv(spark, "support_tickets.csv", support_tickets_schema)

if df is  None: return

df = add_audit_columns(df)

dest = zone_path(BRONZE_PATH, "support_tickets")

write_parquet(df, dest, partition_cols=["severity"])

  

def  ingest_marketing_touches_to_bronze(spark: SparkSession) -> None:

df = _read_landing_csv(spark, "marketing_touches.csv", marketing_touches_schema)

if df is  None: return

df = add_audit_columns(df)

dest = zone_path(BRONZE_PATH, "marketing_touches")

write_parquet(df, dest, partition_cols=["channel"])

  

def  ingest_nps_surveys_to_bronze(spark: SparkSession) -> None:

df = _read_landing_csv(spark, "nps_surveys.csv", nps_surveys_schema)

if df is  None: return

df = add_audit_columns(df)

dest = zone_path(BRONZE_PATH, "nps_surveys")

write_parquet(df, dest, partition_cols=["survey_date"])

  

def  ingest_billing_monthly_to_bronze(spark: SparkSession) -> None:

df = _read_landing_csv(spark, "billing_monthly.csv", billing_monthly_schema)

if df is  None: return

df = add_audit_columns(df)

dest = zone_path(BRONZE_PATH, "billing_monthly")

write_parquet(df, dest, partition_cols=["month"])

```

  

### Ingesta Streaming

Lectura de JSONs en streaming con transformación de fechas, aplicación de *watermarking* de 1 día y deduplicación por `event_id` para garantizar integridad.

  

*Fragmento de código (`bronze_stream.py`):*

```python

def  start_usage_events_to_bronze(spark: SparkSession):

df_stream = create_usage_events_stream(spark)

# Transformaciones: timestamp -> event_ts, watermark, dedupe

df_bronze = transform_usage_events_bronze(df_stream)

  

dest_path = zone_path(BRONZE_PATH, "usage_events")

checkpoint_path = BRONZE_PATH / "_checkpoints" / "usage_events"

  

query = (

df_bronze

.writeStream

.format("parquet")

.option("checkpointLocation", str(checkpoint_path))

.option("path", str(dest_path))

.partitionBy("event_date")

.outputMode("append")

.start()

)

return query
```
### Resultado de la ingesta Streaming:

```python console
[INFO] Comenzando Speed Layer...
[INFO] Dimensión Organizaciones cacheada: 80 registros.
[INFO] Streaming ejecutandose -> /content/datalake/gold/org_daily_usage_by_service_speed
Streaming Speed → Gold
ID: a3016673-1db2-49d6-a7f8-27d726d023e7
Nombre: None
Activo: True
[STREAM 0] Reporte 
 Input: 360 eventos
Validos: 359
Dropped (Cost < -0.01): 1 (0.3%)
todo pasado a Gold de Speed layer
[STREAM 1] Reporte 
 Input: 360 eventos
Validos: 358
Dropped (Cost < -0.01): 2 (0.6%)
todo pasado a Gold de Speed layer
[STREAM 2] Reporte 
 Input: 11 eventos
Validos: 11
todo pasado a Gold de Speed layer
[STREAM 3] Reporte 
 Input: 7 eventos
Validos: 7
todo pasado a Gold de Speed layer
 Progreso del streaming
{'id': 'a3016673-1db2-49d6-a7f8-27d726d023e7', 'runId': '7f48b7be-d5b7-47dc-b5c2-63ed1b19bef1', 'name': None, 'timestamp': '2025-12-07T19:21:25.000Z', 'batchId': 3, 'numInputRows': 360, 'inputRowsPerSecond': 150.9433962264151, 'processedRowsPerSecond': 173.49397590361446, 'durationMs': {'addBatch': 1863, 'commitOffsets': 56, 'getBatch': 11, 'latestOffset': 54, 'queryPlanning': 32, 'triggerExecution': 2075, 'walCommit': 54}, 'eventTime': {'avg': '2025-08-02T03:04:24.833Z', 'max': '2025-08-31T23:51:00.000Z', 'min': '2025-07-03T00:48:00.000Z', 'watermark': '2025-08-30T23:51:00.000Z'}, 'stateOperators': [{'operatorName': 'dedupe', 'numRowsTotal': 2410, 'numRowsUpdated': 25, 'allUpdatesTimeMs': 194, 'numRowsRemoved': 0, 'allRemovalsTimeMs': 0, 'commitTimeMs': 969, 'memoryUsedBytes': 481296, 'numRowsDroppedByWatermark': 1142, 'numShufflePartitions': 13, 'numStateStoreInstances': 13, 'customMetrics': {'loadedMapCacheHitCount': 74, 'loadedMapCacheMissCount': 40, 'numDroppedDuplicateRows': 0, 'stateOnCurrentVersionSizeBytes': 445792}}], 'sources': [{'description': 'FileStreamSource[file:/content/datalake/landing/usage_events_stream]', 'startOffset': {'logOffset': 2}, 'endOffset': {'logOffset': 3}, 'latestOffset': None, 'numInputRows': 360, 'inputRowsPerSecond': 150.9433962264151, 'processedRowsPerSecond': 173.49397590361446}], 'sink': {'description': 'ForeachBatchSink', 'numOutputRows': -1}}
aparando stream...

 cheque de Speed Layer ---
Ruta: /content/datalake/gold/org_daily_usage_by_service_speed, Total acumulado en disco: 727
funcionando todo OK (Datos persistidos correctamente)
+------------+---------------+------------+--------------+--------------+---------------+----------+
|org_id      |org_name       |service_name|daily_cost_usd|daily_requests|daily_carbon_kg|event_date|
+------------+---------------+------------+--------------+--------------+---------------+----------+
|org_pnsm43d8|Delta Labs 70  |compute     |11.6645       |130.0000      |0.026          |2025-08-13|
|org_dhylurtp|Nimbus Cloud 76|compute     |11.9243       |133.0000      |0.0266         |2025-08-13|
|org_5iqvnb4g|Gamma Labs 73  |networking  |0.0092        |0.0000        |2.05E-4        |2025-08-13|
+------------+---------------+------------+--------------+--------------+---------------+----------+
only showing top 3 rows

Streaming Speed → Gold parado.
```

  

### 5.3 Reglas de Calidad y Evidencias

Se verifica unicidad de claves primarias y completitud de campos de auditoría. En una unica celda se chequeo la integridad de todo lo realizado en las capas Bronze, Silver y Gold.

*Salida de ejecución (`audit.py`):*

```python console

[2025-12-07 19:20:51] [RUN] Haciendo quality control...  [2025-12-07 19:20:51] [RUN] Iniciando Auditoría completa de capa Bronze... chequeo Bronze: customers_orgs
registros totales: 80
duplicados en PK (org_id): 0
nulos en ingest_ts: 0
Resultado: todo ok!!

 chequeo Bronze: users
registros totales: 800
duplicados en PK (user_id): 0
nulos en ingest_ts: 0
Resultado: todo ok!!

 chequeo Bronze: resources
registros totales: 400
duplicados en PK (resource_id): 0
nulos en ingest_ts: 0
Resultado: todo ok!!

 chequeo Bronze: support_tickets
registros totales: 1000
duplicados en PK (ticket_id): 0
nulos en ingest_ts: 0
Resultado: todo ok!!

 chequeo Bronze: marketing_touches
registros totales: 1500
duplicados en PK (touch_id): 0
nulos en ingest_ts: 0
Resultado: todo ok!!

 chequeo Bronze: billing_monthly
registros totales: 240
duplicados en PK (invoice_id): 0
nulos en ingest_ts: 0
Resultado: todo ok!!

 chequeo Bronze: nps_surveys
registros totales: 92
duplicados en PK (org_id): 32
nulos en ingest_ts: 0
Resultado: revisar data por posibles duplicados [2025-12-07 19:20:58] [OK] Auditoría Bronze finalizada.  [2025-12-07 19:20:58] [RUN] Iniciando Auditoría de Calidad Silver... Chequeop Silver:
Total: 758
Aceptados (Silver): 755
Rechazados (cuarentena): 3 (0.40%)
CALIDAD: ACEPTABLE
ejemplo de rechazo:
+----------------+-------------------------+
|event_id        |quarantine_reason        |
+----------------+-------------------------+
|evt_faubjbtabmwl|cost_negative_or_null_org|
+----------------+-------------------------+
only showing top 1 row


Calidad de Datos (Silver Batch)

 Estadisticas:
Total Procesado: 758
Aceptados (Silver): 755 (99.60%)
Rechazados (Quarantine): 3 (0.40%)

resultado:
Aceptable- rechazo  bajo y esperado.

 Muestra de registros en Cuarentena (top 5):
+----------------+------------------+------------+-------------------------+
|event_id        |cost_usd_increment|org_id      |quarantine_reason        |
+----------------+------------------+------------+-------------------------+
|evt_faubjbtabmwl|-0.0602           |org_cvs4f8cg|cost_negative_or_null_org|
|evt_qniow8ymxwd6|-0.2446           |org_i7p5tb94|cost_negative_or_null_org|
|evt_bbmth9hzpa6e|-0.3656           |org_n9j2qp89|cost_negative_or_null_org|
+----------------+------------------+------------+-------------------------+ [2025-12-07 19:21:06] [OK] Auditoría Silver finalizada.  [2025-12-07 19:21:06] [RUN] Iniciando Auditoría de Marts Gold... Chequeo Gold: org_daily_usage_by_service
registros Totales (Agregados): 727
Costos Negativos detectados: 0

 Chequeo Gold: org_daily_support_metrics
registros Totales (Agregados): 944
Costos Negativos detectados: 0

 Chequeo Gold: org_daily_genai_usage
registros Totales (Agregados): 77
Costos Negativos detectados: 0 [2025-12-07 19:21:11] [OK] Auditoría Gold finalizada.  [2025-12-07 19:21:11] [OK] Fin demo Batch Layer (Bronze CSV → Silver → Gold)

```  

---

  

## Transformaciones ( Etapa Silver)

  

En la capa Silver (`run_silver_batch`), se realizan transformaciones críticas de limpieza y enriquecimiento. Se utiliza un **Broadcast Join** para cruzar los eventos masivos con los datos de organizaciones.

  

**Cálculos realizados:**

*  **Suma de Costos:** Agregación de `cost_usd_increment` como `daily_cost_usd`.

*  **Conteo de Requests:** Suma condicional donde `metric == 'requests'`.

*  **Cálculo de Carbono:** Acumulación directa de `carbon_kg`.

*  **KPIs Derivados:**  `cost_per_request` y `carbon_per_dollar`.

*  **Soporte:** Conteo de `sla_breached` para obtener la tasa de incumplimiento (`sla_breach_rate`).

  

*Fragmento de código (Reglas de Calidad y Cuarentena):*

```python

# Reglas: Costo >= -0.01 y org_id no nulo

dq_condition = (F.col("cost_usd_increment") >= -0.01) & (F.col("org_id").isNotNull())

good_df = enriched_df.filter(dq_condition)

bad_df = enriched_df.filter(~dq_condition)

  

# Logica Left Anti para evitar duplicados en cuarentena

existing_quarantine = read_parquet(spark, quarantine_dest)

unique_bad_df = bad_df.join(existing_quarantine, on="event_id", how="left_anti")

```

  ### Resultado de la ingesta + Silver a gold (celda demo)
```python console
[OK] Dataset descomprimido en /content/datalake/landing
[2025-12-07 19:19:40] [INFO] PROJECT_ROOT: {PROJECT_ROOT}  [2025-12-07 19:19:40] [INFO] Spark version: 3.5.1  [2025-12-07 19:19:40] [RUN] Generando Bronze Batch (Maestros)... [OK] Dataset descomprimido en /content/datalake/landing

[BATCH] Iniciando Ingesta a Bronze (7 Maestros)... 

Procesando Archivos: 100%

 7/7 [00:19<00:00,  1.83s/tablas]

[INFO] Leyendo /content/datalake/landing/customers_orgs.csv
[OK] Bronze customers_orgs -> /content/datalake/bronze/customers_orgs
[INFO] Leyendo /content/datalake/landing/users.csv
[OK] Bronze users -> /content/datalake/bronze/users
[INFO] Leyendo /content/datalake/landing/resources.csv
[OK] Bronze resources -> /content/datalake/bronze/resources
[INFO] Leyendo /content/datalake/landing/support_tickets.csv
[OK] Bronze support_tickets -> /content/datalake/bronze/support_tickets
[INFO] Leyendo /content/datalake/landing/marketing_touches.csv
[OK] Bronze marketing_touches -> /content/datalake/bronze/marketing_touches
[INFO] Leyendo /content/datalake/landing/nps_surveys.csv
[OK] Bronze nps_surveys -> /content/datalake/bronze/nps_surveys
[INFO] Leyendo /content/datalake/landing/billing_monthly.csv
[OK] Bronze billing_monthly -> /content/datalake/bronze/billing_monthly
[2025-12-07 19:20:03] [OK] Capa Bronze finalizada correctamente  [2025-12-07 19:20:03] [RUN] Generando Bronze Stream (Eventos)... [INFO] Streaming usage_events -> /content/datalake/bronze/usage_events
[INFO] Checkpoints en /content/datalake/bronze/_checkpoints/usage_events [2025-12-07 19:20:19] [OK] Stream procesado.  [2025-12-07 19:20:19] [RUN] Generando Silver... [INFO] Iniciando Silver...
[WARN] Creando cuarentena por primera vez
[OK] Silver Batch completado, todo ok -> /content/datalake/silver/usage_events_enriched [2025-12-07 19:20:38] [OK] Silver finalizado  [2025-12-07 19:20:38] [RUN] ejecuntando proceso Gold... [OK] Gold Batch (FinOps) -> /content/datalake/gold/org_daily_usage_by_service
[OK] Gold Batch (Support) -> /content/datalake/gold/org_daily_support_metrics
[OK] Gold Batch (GenAI) -> /content/datalake/gold/org_daily_genai_usage [2025-12-07 19:20:51] [OK] proceso Gold finalizado
```


---

  

## Modelado Gold y Serving en AstraDB (Cassandra)

  

### Diseño por consulta (query-first)

Se diseñaron tablas con claves compuestas (`org_id` como Partition Key) para satisfacer consultas de dashboards por cliente y rango de fechas.  

El modelo de datos se diseñó siguiendo la metodología *Query-First* de Cassandra, optimizando las tablas para los filtros específicos de los dashboards de negocio.

  

**1. Caso de Uso: FinOps (Tabla `org_daily_usage_by_service`)**

*  **Partition Key:**  `org_id`.

*  *Justificación:* Los dashboards siempre filtran por un cliente específico. Esto asegura que todos los datos de una organización residan en el mismo nodo, maximizando la velocidad de lectura.

*  **Clustering Keys:**  `usage_date` (DESC), `service_name` (ASC).

*  *Justificación:* El ordenamiento por fecha descendente permite obtener rápidamente los datos "últimos 30 días" sin ordenar en memoria. El servicio asegura la unicidad del registro diario.

*  **TTL:** No aplica. Se requiere retención histórica indefinida para auditorías de facturación y comparativas anuales (Year-over-Year).

  

**2. Caso de Uso: Soporte (Tabla `org_daily_support_metrics`)**

*  **Partition Key:**  `org_id`.

*  **Clustering Key:**  `ticket_date` (DESC).

*  *Justificación:* Permite consultas de rango eficientes para un cliente dado.

  

**3. Caso de Uso: Producto/GenAI (Tabla `org_daily_genai_usage`)**

*  **Partition Key:**  `org_id`.

*  **Clustering Keys:**  `event_date` (DESC), `service_name`.

*  *Justificación:* Segrega el tráfico de alto volumen de GenAI para no afectar la lectura de costos generales.
  

### Scripts CQL

A continuación se incluyen todas las *queries* CQL implementadas en `cassandra_utils.py` para la creación de las tablas de FinOps, Soporte y GenAI.


```sql

-- 1. Tabla FinOps: Costos y métricas operativas diarias por servicio

CREATE  TABLE  IF  NOT  EXISTS  "Cloud_analytics_db".org_daily_usage_by_service (

org_id text,

usage_date date,

service_name  text,

daily_cost_usd double,

daily_requests double,

daily_carbon_kg double,

cost_per_request double,

carbon_per_dollar double,

PRIMARY KEY ((org_id), usage_date, service_name)

) WITH CLUSTERING ORDER BY (usage_date DESC, service_name  ASC);

  
-- 2. Tabla de Soporte: Métricas de tickets, SLAs y satisfacción

CREATE  TABLE  IF  NOT  EXISTS  "Cloud_analytics_db".org_daily_support_metrics (

org_id text,

ticket_date date,

total_tickets int,

critical_tickets int,

sla_breached_count int,

avg_csat double,

sla_breach_rate double,

PRIMARY KEY ((org_id), ticket_date)

) WITH CLUSTERING ORDER BY (ticket_date DESC);

-- 3. Tabla de GenAI: Consumo específico de tokens y costos de IA

CREATE  TABLE  IF  NOT  EXISTS  "Cloud_analytics_db".org_daily_genai_usage (

org_id text,

event_date date,

service_name  text,

genai_daily_cost double,

genai_requests_count double,

PRIMARY KEY ((org_id), event_date, service_name)

) WITH CLUSTERING ORDER BY (event_date DESC);
 
```

### Carga a Cassandra

*Evidencia de carga a Cassandra (`cassandra_loader.py` output):*

```python console
[SERVING] Iniciando carga a AstraDB...
[CASSANDRA] Creando esquema en keyspace 'Cloud_analytics_db'...
[CASSANDRA] Esquema verificado: 3 Tablas listas.
   -> cargando tabla 'org_daily_usage_by_service' (731 registros)...

Subiendo org_daily_usage_by_service: 100%

 731/731 [00:15<00:00, 46.48rows/s]

      [OK] Carga completada.
   -> cargando tabla 'org_daily_support_metrics' (944 registros)...

Subiendo org_daily_support_metrics: 100%

 944/944 [00:20<00:00, 46.19rows/s]

      [OK] Carga completada.
   -> cargando tabla 'org_daily_genai_usage' (78 registros)...

Subiendo org_daily_genai_usage: 100%

 78/78 [00:01<00:00, 47.25rows/s]

      [OK] Carga completada.
```

### Chequeo de tablas

```python console
chequeo de carga en Cassandra (Keyspace: Cloud_analytics_db)

Tabla: `org_daily_usage_by_service`
- Total de registros : 876

Muestra de datos:

| org_id       | usage_date   | service_name   |   carbon_per_dollar |   cost_per_request |   daily_carbon_kg |   daily_cost_usd |   daily_requests |
|:-------------|:-------------|:---------------|--------------------:|-------------------:|------------------:|-----------------:|-----------------:|
| org_c11ertj5 | 2025-08-31   | compute        |          0.00326907 |          0.0638383 |          0.025043 |           7.6606 |              120 |
| org_c11ertj5 | 2025-08-31   | database       |          0.00413361 |          0.0504168 |          0.0248   |           5.9996 |              119 |
| org_c11ertj5 | 2025-08-31   | genai          |          0.00157653 |          0.127047  |          0.049072 |          31.1266 |              245 |
| org_c11ertj5 | 2025-08-18   | compute        |          0.00292469 |        nan         |          0.000167 |           0.0571 |                0 |
| org_c11ertj5 | 2025-08-13   | database       |          0.00418779 |        nan         |          0.000446 |           0.1065 |                0 |

---

Tabla: `org_daily_support_metrics`
- Total de registros : 944

Muestra de datos:

| org_id       | ticket_date   |   avg_csat |   critical_tickets |   sla_breach_rate |   sla_breached_count |   total_tickets |
|:-------------|:--------------|-----------:|-------------------:|------------------:|---------------------:|----------------:|
| org_c11ertj5 | 2025-08-30    |          3 |                  0 |                 0 |                    0 |               1 |
| org_c11ertj5 | 2025-08-16    |          3 |                  0 |                 0 |                    0 |               1 |
| org_c11ertj5 | 2025-08-07    |          5 |                  0 |                 1 |                    1 |               1 |
| org_c11ertj5 | 2025-08-03    |          2 |                  0 |                 0 |                    0 |               1 |
| org_c11ertj5 | 2025-07-23    |        nan |                  0 |                 0 |                    0 |               1 |

---

Tabla: `org_daily_genai_usage`
- Total de registros : 92

Muestra de datos:

| org_id       | event_date   | service_name   |   genai_daily_cost |   genai_requests_count |
|:-------------|:-------------|:---------------|-------------------:|-----------------------:|
| org_c11ertj5 | 2025-08-31   | genai          |            31.1266 |                    245 |
| org_c11ertj5 | 2025-08-07   | genai          |             0.2865 |                      0 |
| org_c11ertj5 | 2025-07-21   | genai          |            12.6785 |                    110 |
| org_w3zp08j3 | 2025-08-31   | genai          |             0.3548 |                      0 |
| org_kdgigatj | 2025-07-15   | genai          |             0      |                      0 |

---
```

  

---
  

## Idempotencia y Reprocesos

  

La idempotencia se garantiza mediante el diseño de claves primarias naturales en Cassandra, lo que convierte las inserciones en *Upserts*. Se validó ejecutando el proceso de carga dos veces consecutivas y verificando que los conteos no aumentaran.

  *Evidencia (Salida del chequeo de Idempotencia):*

```python console

Prueba de idempotencia 

[1] Contando registros actuales...
    - org_daily_usage_by_service: 876
    - org_daily_support_metrics: 944
    - org_daily_genai_usage: 92

[2] >>> Re-ejecutando Carga (Gold -> Cassandra)...

[SERVING] Iniciando carga masiva a AstraDB...
[CASSANDRA] Creando esquema en keyspace 'Cloud_analytics_db'...
[CASSANDRA] Esquema verificado: 3 Tablas listas.
   -> cargando tabla 'org_daily_usage_by_service' (727 registros)...

Subiendo org_daily_usage_by_service: 100%

 727/727 [00:26<00:00, 27.93rows/s]

      [OK] Carga completada.
   -> cargando tabla 'org_daily_support_metrics' (944 registros)...

Subiendo org_daily_support_metrics: 100%

 944/944 [00:34<00:00, 28.07rows/s]

      [OK] Carga completada.
   -> cargando tabla 'org_daily_genai_usage' (77 registros)...

Subiendo org_daily_genai_usage: 100%

 77/77 [00:02<00:00, 27.60rows/s]

      [OK] Carga completada.

[3] Verificando consistencia...
    - org_daily_usage_by_service: 876 -> 876 [OK]
    - org_daily_support_metrics: 944 -> 944 [OK]
    - org_daily_genai_usage: 92 -> 92 [OK]
--------------------------------------------------
Resultado: 0 Duplicados generados.
```
Como se puede ver, al realizar upserts los duplicados terminan siendo cero, confirmando la idempotencia en la base de Cassandra

---


## Performance

Para garantizar un rendimiento óptimo en un entorno de recursos limitados (Single-Node en Colab) y minimizar la latencia, se implementaron tres estrategias clave de optimización en Spark:

1.  **Broadcast Hash Join (Map-Side Join):** En la capa Silver, al cruzar la tabla de hechos masiva (`usage_events`) con la dimensión pequeña (`customers_orgs`), se forzo un `F.broadcast()`. Esto evita el intercambio de datos (*Shuffle*) a través de la red, enviando una copia completa de la tabla de clientes a la memoria de cada ejecutor, transformando un *SortMergeJoin* en un *BroadcastHashJoin* mucho más rápido.
2.  **Ajuste de Shuffle Partitions:** Se configuró `spark.sql.shuffle.partitions = 4` (el default es 200). Dado que el volumen de datos del MVP es moderado (inferior a 10 GB), mantener 200 particiones generaría una sobrecarga excesiva en la planificación de tareas (*task scheduling overhead*) y fragmentación de archivos pequeños (*small files*). Al reducirlo a 4, se optimizan las tareas, maximizando el output
3.  **Caching en Streaming:** En la *Speed Layer*, se aplicó `.cache()` a la dimensión de organizaciones antes de iniciar el flujo. Sin esto, Spark volvería a leer los archivos Parquet de la capa Bronze en cada *micro-batch* (cada 5 segundos), degradando la latencia I/O. Al mantener la tabla en memoria RAM, el *lookup* es instantáneo.

**Métricas observadas:**
Gracias a estas optimizaciones, el procesamiento de la capa Silver (limpieza y joins) mantuvoun  tiempo de ejecución de **~12 segundos**. Asimismo, la carga a Cassandra mantuvo una tasa de escritura constante de **~45 filas/segundo**, limitada únicamente por el ancho de banda de red hacia AstraDB y no por el procesamiento de Spark.
 
 ---
  
## Consultas mínimas desde AstraDB

Se armaron las siguientes Queries CQL para realizar uan evaluacion del estado de las tablas subidas a Cassandra yver que tan factible es realizar consultas de negocios reales.
Si bien se pedian 5 queries, se realizó una sexta respecto al rema sustentabilidad para asi mostrar mas  posibiliades de las tablas.

 **Queries**

```python
# (1) FinOps: Costos y requests diarios (rango de fechas)
    print("\n1. [FinOps] Costos y requests diarios (Últimos 7 días)")
    query_1 = f"""
        SELECT usage_date, service_name, daily_cost_usd, daily_requests
        FROM "{KEYSPACE}".org_daily_usage_by_service
        WHERE org_id = '{TARGET_ORG}'
        AND usage_date >= '{ref_date - timedelta(days=7)}'
    """
    rows = session.execute(query_1)
    print(f"{'Fecha':<12} | {'Servicio':<15} | {'Costo ($)':<10} | {'Requests'}")
    for r in rows:
        print(f"{str(r.usage_date):<12} | {r.service_name:<15} | {r.daily_cost_usd:<10.2f} | {int(r.daily_requests)}")


    # (2) FinOps: Top 3 servicios mas costosos (ultimos 14 dias)
    print(f"\n2. [FinOps] Top 3 Servicios más costosos (Desde {date_14_days_ago})")
    query_2 = f"""
        SELECT service_name, daily_cost_usd
        FROM "{KEYSPACE}".org_daily_usage_by_service
        WHERE org_id = '{TARGET_ORG}'
        AND usage_date >= '{date_14_days_ago}'
    """
    rows = session.execute(query_2)

    # agregacion client-side
    cost_map = {}
    for r in rows:
        cost_map[r.service_name] = cost_map.get(r.service_name, 0.0) + r.daily_cost_usd

    sorted_services = sorted(cost_map.items(), key=lambda x: x[1], reverse=True)[:3]

    print(f"{'Servicio':<15} | {'Costo Acum. ($)'}")
    for svc, cost in sorted_services:
        print(f"{svc:<15} | {cost:.2f}")


    # (3) Soporte: Tickets criticos y SLA breach rate (30 dias)
    print(f"\n3. [Soporte] Tickets críticos y Tasa SLA Breach (Desde {date_30_days_ago})")
    query_3 = f"""
        SELECT ticket_date, critical_tickets, sla_breach_rate
        FROM "{KEYSPACE}".org_daily_support_metrics
        WHERE org_id = '{TARGET_ORG}'
        AND ticket_date >= '{date_30_days_ago}'
    """
    rows = session.execute(query_3)

    # manejo por si no hay tickets# Proyecto “Cloud Provider Analytics” – Final

  

>  **Formato:** Documento oficial de entrega del MVP final.

  

---

  


-  **Título del proyecto:** Cloud Provider Analytics: Plataforma de Inteligencia de Costos y Operaciones

-  **Integrantes:** [Completar con Nombres]

  
  

## Resumen Ejecutivo

  
Este proyecto  implementa un ecosistema analítico para un proveedor de nube ficticio que necesita integrar datos de **FinOps, Soporte y Producto/GenAI** a partir de fuentes heterogéneas y con evolución de esquema (`schema_version`, aparición de `carbon_kg` y `genai_tokens`) para habilitar analítica diaria y _near real-time_.

Se adopta un arquitectura de solución tipo **Lambda**: pipeline batch para maestros (CRM, facturación, NPS) y pipeline de _Structured Streaming_ para eventos de uso (`usage_events_stream`) sobre un data lake Parquet (Landing → Bronze → Silver → Gold), con publicación final de _marts_ en **AstraDB (Cassandra)** para consumo por herramientas de BI.

El MVP final implementa: 
1.  ingesta batch de CSV a Bronze con esquemas tipados; 
2.  _streaming_ de eventos JSONL con `withWatermark` + `dropDuplicates(event_id)`;
3.  limpieza y conformidad en Silver, incluyendo integración con dimensiones de organizaciones y recursos; 
4. _marts_ Gold para FinOps (`org_daily_usage_by_service`), Soporte (`org_daily_support_metrics`) y GenAI (`org_daily_genai_usage`); 
5.  carga idempotente a Cassandra con driver Python y pruebas explícitas de reproceso; 
6.  consultas de negocio desde AstraDB 

El resultado es un pipeline *end-to-end* que alimenta una *Serving Layer* en AstraDB (Cassandra), permitiendo consultas de alto rendimiento. Los usuarios pueden obtener *insights* inmediatos, como identificar los servicios más costosos de la última semana o correlacionar picos de incidentes críticos con fechas de despliegue, todo validado mediante un dashboard interactivo.


---

  

## Arquitectura Final

  
**1. Diagrama de alto nivel**

El flujo implementa una arquitectura Lambda clásica. Los datos CSV (Maestros) siguen la ruta Batch (Landing $\rightarrow$ Bronze $\rightarrow$ Silver $\rightarrow$ Gold), mientras que los eventos JSON siguen la ruta Speed (Streaming $\rightarrow$ Gold Directo). Ambas rutas convergen en la capa de servicio (Cassandra).

  

![Diagrama de flujo de arquitectura Lambda](https://github.com/Sinnick4r/Cloud_Provider_Analytics_MVP/blob/main/documentacion/Diagrama%20Proyecto.png?raw=true)

  

Se eligió un patrón de **Arquitectura Lambda** para combinar la robustez del procesamiento *Batch* (necesario para la conciliación precisa de facturación histórica y datos maestros) con la baja latencia del *Speed Layer* (crítico para alertas de consumo en tiempo real), permitiendo responder preguntas de negocio complejas sobre costos y operaciones.


**2. Patrón elegido y Decisiones clave**

*  **Particionamiento:**  `hq_region` para maestros estáticos y `event_date` para eventos transaccionales, optimizando la lectura.

*  **Formatos:** Parquet en todo el Data Lake por eficiencia de I/O y soporte de esquemas.

  
**3. Componentes y dependencias**

*  **Procesamiento:** Apache Spark 3.5.1 (PySpark).

*  **Storage:** Sistema de archivos local simulando S3/HDFS.

*  **Serving:** DataStax AstraDB (Cassandra) + Driver Python.

*  **Dependencias:**  `pyspark`, `cassandra-driver`, `astrapy`, `gradio`, `plotly`.

  ---

  

## Datos y Supuestos

  
Los datos provienen de un entorno simulado de proveedor de nube. Se normalizaron timestamps a UTC y montos a decimales. A continuación, el diccionario de datos completo basado en los esquemas definidos en `schemas.py`.

  

**Diccionario de datos clave (extracto):**

  

| Campo | Dataset | Tipo | Descripción | Observaciones |
|---|---|---|---|---|
| `org_id` | customers_orgs | String | ID único de la organización | Clave primaria y de partición en Cassandra. |
| `hq_region` | customers_orgs | String | Región de la sede | Usado para particionamiento en Bronze. |
| `plan_tier` | customers_orgs | String | Nivel de plan (Free, Pro, Enterprise) | Usado para segmentación en Silver. |
| `user_id` | users | String | ID único del usuario | - |
| `role` | users | String | Rol del usuario (Admin, Viewer) | - |
| `resource_id` | resources | String | ID del recurso en la nube | - |
| `service` | resources | String | Nombre del servicio (compute, storage) | - |
| `ticket_id` | support_tickets | String | ID del ticket de soporte | - |
| `severity` | support_tickets | String | Criticidad (Low, Medium, Critical) | Usado para métricas de SLA. |
| `sla_breached` | support_tickets | Boolean | Flag de incumplimiento de SLA | True si se venció el plazo. |
| `invoice_id` | billing_monthly | String | ID de la factura mensual | - |
| `subtotal` | billing_monthly | Decimal | Monto antes de impuestos | - |
| `event_id` | usage_events | String | ID único del evento de uso | Clave para deduplicación. |
| `cost_usd_increment` | usage_events | Decimal | Costo incremental del evento | Regla de calidad: $\ge -0.01$. |
| `carbon_kg` | usage_events | Double | Huella de carbono estimada | Métrica ESG. |
| `genai_tokens` | usage_events | Long | Tokens consumidos (si aplica) | Métrica específica GenAI (derivada). |

  
---

## Data Lake: Zonas y Particionado

  
El Data Lake se organiza estrictamente en zonas para garantizar la gobernanza y calidad del dato:

*  **Landing:** Datos crudos inmutables (CSV/JSON).

*  **Bronze:** Raw estándar, mismo grano que la fuente, tipificación explícita, dedupe por `event_id` y columnas técnicas `ingest_ts`, `source_file`

*  **Silver:** datos limpios y conformados con joins a dimensiones y tratamiento de nulos/outliers . Particionado por `event_date`.

*  **Gold:** Marts de negocio agregados. Particionado por `event_date` o `ticket_date`.

*  **Quarantine:** Zona aislada para registros que no cumplen reglas de calidad (ej. costos negativos).
  

![Diagrama de Pipelines de Datos](https://github.com/Sinnick4r/Cloud_Provider_Analytics_MVP/blob/main/documentacion/Pipelines.png?raw=true)

###  Directorios del datalake
  ![Diagrama de Pipelines de Datos](https://github.com/Sinnick4r/Cloud_Provider_Analytics_MVP/blob/main/documentacion/estructura%20de%20directorios.png?raw=true)

---

  

## Ingesta y Calidad de Datos

  

### Ingesta Batch

Se implementaron funciones dedicadas para cada dataset maestro, aplicando lectura con esquema estricto, adición de columnas de auditoría y escritura particionada en formato Parquet.

  

*Fragmento de código (`bronze_batch.py`):*

```python

def  ingest_customers_orgs_to_bronze(spark: SparkSession) -> None:

df = _read_landing_csv(spark, "customers_orgs.csv", customers_orgs_schema)

if df is  None: return

df = add_audit_columns(df)

dest = zone_path(BRONZE_PATH, "customers_orgs")

write_parquet(df, dest, partition_cols=["hq_region"])

  

def  ingest_users_to_bronze(spark: SparkSession) -> None:

df = _read_landing_csv(spark, "users.csv", users_schema)

if df is  None: return

df = add_audit_columns(df)

dest = zone_path(BRONZE_PATH, "users")

write_parquet(df, dest, partition_cols=["role"])

  

def  ingest_resources_to_bronze(spark: SparkSession) -> None:

df = _read_landing_csv(spark, "resources.csv", resources_schema)

if df is  None: return

df = add_audit_columns(df)

dest = zone_path(BRONZE_PATH, "resources")

write_parquet(df, dest, partition_cols=["region"])

  

def  ingest_support_tickets_to_bronze(spark: SparkSession) -> None:

df = _read_landing_csv(spark, "support_tickets.csv", support_tickets_schema)

if df is  None: return

df = add_audit_columns(df)

dest = zone_path(BRONZE_PATH, "support_tickets")

write_parquet(df, dest, partition_cols=["severity"])

  

def  ingest_marketing_touches_to_bronze(spark: SparkSession) -> None:

df = _read_landing_csv(spark, "marketing_touches.csv", marketing_touches_schema)

if df is  None: return

df = add_audit_columns(df)

dest = zone_path(BRONZE_PATH, "marketing_touches")

write_parquet(df, dest, partition_cols=["channel"])

  

def  ingest_nps_surveys_to_bronze(spark: SparkSession) -> None:

df = _read_landing_csv(spark, "nps_surveys.csv", nps_surveys_schema)

if df is  None: return

df = add_audit_columns(df)

dest = zone_path(BRONZE_PATH, "nps_surveys")

write_parquet(df, dest, partition_cols=["survey_date"])

  

def  ingest_billing_monthly_to_bronze(spark: SparkSession) -> None:

df = _read_landing_csv(spark, "billing_monthly.csv", billing_monthly_schema)

if df is  None: return

df = add_audit_columns(df)

dest = zone_path(BRONZE_PATH, "billing_monthly")

write_parquet(df, dest, partition_cols=["month"])

```

  

### Ingesta Streaming

Lectura de JSONs en streaming con transformación de fechas, aplicación de *watermarking* de 1 día y deduplicación por `event_id` para garantizar integridad.

  

*Fragmento de código (`bronze_stream.py`):*

```python

def  start_usage_events_to_bronze(spark: SparkSession):

df_stream = create_usage_events_stream(spark)

# Transformaciones: timestamp -> event_ts, watermark, dedupe

df_bronze = transform_usage_events_bronze(df_stream)

  

dest_path = zone_path(BRONZE_PATH, "usage_events")

checkpoint_path = BRONZE_PATH / "_checkpoints" / "usage_events"

  

query = (

df_bronze

.writeStream

.format("parquet")

.option("checkpointLocation", str(checkpoint_path))

.option("path", str(dest_path))

.partitionBy("event_date")

.outputMode("append")

.start()

)

return query
```
### Resultado de la ingesta Streaming:

```python console
[INFO] Comenzando Speed Layer...
[INFO] Dimensión Organizaciones cacheada: 80 registros.
[INFO] Streaming ejecutandose -> /content/datalake/gold/org_daily_usage_by_service_speed
Streaming Speed → Gold
ID: a3016673-1db2-49d6-a7f8-27d726d023e7
Nombre: None
Activo: True
[STREAM 0] Reporte 
 Input: 360 eventos
Validos: 359
Dropped (Cost < -0.01): 1 (0.3%)
todo pasado a Gold de Speed layer
[STREAM 1] Reporte 
 Input: 360 eventos
Validos: 358
Dropped (Cost < -0.01): 2 (0.6%)
todo pasado a Gold de Speed layer
[STREAM 2] Reporte 
 Input: 11 eventos
Validos: 11
todo pasado a Gold de Speed layer
[STREAM 3] Reporte 
 Input: 7 eventos
Validos: 7
todo pasado a Gold de Speed layer
 Progreso del streaming
{'id': 'a3016673-1db2-49d6-a7f8-27d726d023e7', 'runId': '7f48b7be-d5b7-47dc-b5c2-63ed1b19bef1', 'name': None, 'timestamp': '2025-12-07T19:21:25.000Z', 'batchId': 3, 'numInputRows': 360, 'inputRowsPerSecond': 150.9433962264151, 'processedRowsPerSecond': 173.49397590361446, 'durationMs': {'addBatch': 1863, 'commitOffsets': 56, 'getBatch': 11, 'latestOffset': 54, 'queryPlanning': 32, 'triggerExecution': 2075, 'walCommit': 54}, 'eventTime': {'avg': '2025-08-02T03:04:24.833Z', 'max': '2025-08-31T23:51:00.000Z', 'min': '2025-07-03T00:48:00.000Z', 'watermark': '2025-08-30T23:51:00.000Z'}, 'stateOperators': [{'operatorName': 'dedupe', 'numRowsTotal': 2410, 'numRowsUpdated': 25, 'allUpdatesTimeMs': 194, 'numRowsRemoved': 0, 'allRemovalsTimeMs': 0, 'commitTimeMs': 969, 'memoryUsedBytes': 481296, 'numRowsDroppedByWatermark': 1142, 'numShufflePartitions': 13, 'numStateStoreInstances': 13, 'customMetrics': {'loadedMapCacheHitCount': 74, 'loadedMapCacheMissCount': 40, 'numDroppedDuplicateRows': 0, 'stateOnCurrentVersionSizeBytes': 445792}}], 'sources': [{'description': 'FileStreamSource[file:/content/datalake/landing/usage_events_stream]', 'startOffset': {'logOffset': 2}, 'endOffset': {'logOffset': 3}, 'latestOffset': None, 'numInputRows': 360, 'inputRowsPerSecond': 150.9433962264151, 'processedRowsPerSecond': 173.49397590361446}], 'sink': {'description': 'ForeachBatchSink', 'numOutputRows': -1}}
aparando stream...

 cheque de Speed Layer ---
Ruta: /content/datalake/gold/org_daily_usage_by_service_speed, Total acumulado en disco: 727
funcionando todo OK (Datos persistidos correctamente)
+------------+---------------+------------+--------------+--------------+---------------+----------+
|org_id      |org_name       |service_name|daily_cost_usd|daily_requests|daily_carbon_kg|event_date|
+------------+---------------+------------+--------------+--------------+---------------+----------+
|org_pnsm43d8|Delta Labs 70  |compute     |11.6645       |130.0000      |0.026          |2025-08-13|
|org_dhylurtp|Nimbus Cloud 76|compute     |11.9243       |133.0000      |0.0266         |2025-08-13|
|org_5iqvnb4g|Gamma Labs 73  |networking  |0.0092        |0.0000        |2.05E-4        |2025-08-13|
+------------+---------------+------------+--------------+--------------+---------------+----------+
only showing top 3 rows

Streaming Speed → Gold parado.
```

  

### 5.3 Reglas de Calidad y Evidencias

Se verifica unicidad de claves primarias y completitud de campos de auditoría. En una unica celda se chequeo la integridad de todo lo realizado en las capas Bronze, Silver y Gold.

*Salida de ejecución (`audit.py`):*

```python console

[2025-12-07 19:20:51] [RUN] Haciendo quality control...  [2025-12-07 19:20:51] [RUN] Iniciando Auditoría completa de capa Bronze... chequeo Bronze: customers_orgs
registros totales: 80
duplicados en PK (org_id): 0
nulos en ingest_ts: 0
Resultado: todo ok!!

 chequeo Bronze: users
registros totales: 800
duplicados en PK (user_id): 0
nulos en ingest_ts: 0
Resultado: todo ok!!

 chequeo Bronze: resources
registros totales: 400
duplicados en PK (resource_id): 0
nulos en ingest_ts: 0
Resultado: todo ok!!

 chequeo Bronze: support_tickets
registros totales: 1000
duplicados en PK (ticket_id): 0
nulos en ingest_ts: 0
Resultado: todo ok!!

 chequeo Bronze: marketing_touches
registros totales: 1500
duplicados en PK (touch_id): 0
nulos en ingest_ts: 0
Resultado: todo ok!!

 chequeo Bronze: billing_monthly
registros totales: 240
duplicados en PK (invoice_id): 0
nulos en ingest_ts: 0
Resultado: todo ok!!

 chequeo Bronze: nps_surveys
registros totales: 92
duplicados en PK (org_id): 32
nulos en ingest_ts: 0
Resultado: revisar data por posibles duplicados [2025-12-07 19:20:58] [OK] Auditoría Bronze finalizada.  [2025-12-07 19:20:58] [RUN] Iniciando Auditoría de Calidad Silver... Chequeop Silver:
Total: 758
Aceptados (Silver): 755
Rechazados (cuarentena): 3 (0.40%)
CALIDAD: ACEPTABLE
ejemplo de rechazo:
+----------------+-------------------------+
|event_id        |quarantine_reason        |
+----------------+-------------------------+
|evt_faubjbtabmwl|cost_negative_or_null_org|
+----------------+-------------------------+
only showing top 1 row


Calidad de Datos (Silver Batch)

 Estadisticas:
Total Procesado: 758
Aceptados (Silver): 755 (99.60%)
Rechazados (Quarantine): 3 (0.40%)

resultado:
Aceptable- rechazo  bajo y esperado.

 Muestra de registros en Cuarentena (top 5):
+----------------+------------------+------------+-------------------------+
|event_id        |cost_usd_increment|org_id      |quarantine_reason        |
+----------------+------------------+------------+-------------------------+
|evt_faubjbtabmwl|-0.0602           |org_cvs4f8cg|cost_negative_or_null_org|
|evt_qniow8ymxwd6|-0.2446           |org_i7p5tb94|cost_negative_or_null_org|
|evt_bbmth9hzpa6e|-0.3656           |org_n9j2qp89|cost_negative_or_null_org|
+----------------+------------------+------------+-------------------------+ [2025-12-07 19:21:06] [OK] Auditoría Silver finalizada.  [2025-12-07 19:21:06] [RUN] Iniciando Auditoría de Marts Gold... Chequeo Gold: org_daily_usage_by_service
registros Totales (Agregados): 727
Costos Negativos detectados: 0

 Chequeo Gold: org_daily_support_metrics
registros Totales (Agregados): 944
Costos Negativos detectados: 0

 Chequeo Gold: org_daily_genai_usage
registros Totales (Agregados): 77
Costos Negativos detectados: 0 [2025-12-07 19:21:11] [OK] Auditoría Gold finalizada.  [2025-12-07 19:21:11] [OK] Fin demo Batch Layer (Bronze CSV → Silver → Gold)

```  

---

  

## Transformaciones ( Etapa Silver)

  

En la capa Silver (`run_silver_batch`), se realizan transformaciones críticas de limpieza y enriquecimiento. Se utiliza un **Broadcast Join** para cruzar los eventos masivos con los datos de organizaciones.

  

**Cálculos realizados:**

*  **Suma de Costos:** Agregación de `cost_usd_increment` como `daily_cost_usd`.

*  **Conteo de Requests:** Suma condicional donde `metric == 'requests'`.

*  **Cálculo de Carbono:** Acumulación directa de `carbon_kg`.

*  **KPIs Derivados:**  `cost_per_request` y `carbon_per_dollar`.

*  **Soporte:** Conteo de `sla_breached` para obtener la tasa de incumplimiento (`sla_breach_rate`).

  

*Fragmento de código (Reglas de Calidad y Cuarentena):*

```python

# Reglas: Costo >= -0.01 y org_id no nulo

dq_condition = (F.col("cost_usd_increment") >= -0.01) & (F.col("org_id").isNotNull())

good_df = enriched_df.filter(dq_condition)

bad_df = enriched_df.filter(~dq_condition)

  

# Logica Left Anti para evitar duplicados en cuarentena

existing_quarantine = read_parquet(spark, quarantine_dest)

unique_bad_df = bad_df.join(existing_quarantine, on="event_id", how="left_anti")

```

  ### Resultado de la ingesta + Silver a gold (celda demo)
```python console
[OK] Dataset descomprimido en /content/datalake/landing
[2025-12-07 19:19:40] [INFO] PROJECT_ROOT: {PROJECT_ROOT}  [2025-12-07 19:19:40] [INFO] Spark version: 3.5.1  [2025-12-07 19:19:40] [RUN] Generando Bronze Batch (Maestros)... [OK] Dataset descomprimido en /content/datalake/landing

[BATCH] Iniciando Ingesta a Bronze (7 Maestros)... 

Procesando Archivos: 100%

 7/7 [00:19<00:00,  1.83s/tablas]

[INFO] Leyendo /content/datalake/landing/customers_orgs.csv
[OK] Bronze customers_orgs -> /content/datalake/bronze/customers_orgs
[INFO] Leyendo /content/datalake/landing/users.csv
[OK] Bronze users -> /content/datalake/bronze/users
[INFO] Leyendo /content/datalake/landing/resources.csv
[OK] Bronze resources -> /content/datalake/bronze/resources
[INFO] Leyendo /content/datalake/landing/support_tickets.csv
[OK] Bronze support_tickets -> /content/datalake/bronze/support_tickets
[INFO] Leyendo /content/datalake/landing/marketing_touches.csv
[OK] Bronze marketing_touches -> /content/datalake/bronze/marketing_touches
[INFO] Leyendo /content/datalake/landing/nps_surveys.csv
[OK] Bronze nps_surveys -> /content/datalake/bronze/nps_surveys
[INFO] Leyendo /content/datalake/landing/billing_monthly.csv
[OK] Bronze billing_monthly -> /content/datalake/bronze/billing_monthly
[2025-12-07 19:20:03] [OK] Capa Bronze finalizada correctamente  [2025-12-07 19:20:03] [RUN] Generando Bronze Stream (Eventos)... [INFO] Streaming usage_events -> /content/datalake/bronze/usage_events
[INFO] Checkpoints en /content/datalake/bronze/_checkpoints/usage_events [2025-12-07 19:20:19] [OK] Stream procesado.  [2025-12-07 19:20:19] [RUN] Generando Silver... [INFO] Iniciando Silver...
[WARN] Creando cuarentena por primera vez
[OK] Silver Batch completado, todo ok -> /content/datalake/silver/usage_events_enriched [2025-12-07 19:20:38] [OK] Silver finalizado  [2025-12-07 19:20:38] [RUN] ejecuntando proceso Gold... [OK] Gold Batch (FinOps) -> /content/datalake/gold/org_daily_usage_by_service
[OK] Gold Batch (Support) -> /content/datalake/gold/org_daily_support_metrics
[OK] Gold Batch (GenAI) -> /content/datalake/gold/org_daily_genai_usage [2025-12-07 19:20:51] [OK] proceso Gold finalizado
```


---

  

## Modelado Gold y Serving en AstraDB (Cassandra)

  

### Diseño por consulta (query-first)

Se diseñaron tablas con claves compuestas (`org_id` como Partition Key) para satisfacer consultas de dashboards por cliente y rango de fechas.  

El modelo de datos se diseñó siguiendo la metodología *Query-First* de Cassandra, optimizando las tablas para los filtros específicos de los dashboards de negocio.

  

**1. Caso de Uso: FinOps (Tabla `org_daily_usage_by_service`)**

*  **Partition Key:**  `org_id`.

*  *Justificación:* Los dashboards siempre filtran por un cliente específico. Esto asegura que todos los datos de una organización residan en el mismo nodo, maximizando la velocidad de lectura.

*  **Clustering Keys:**  `usage_date` (DESC), `service_name` (ASC).

*  *Justificación:* El ordenamiento por fecha descendente permite obtener rápidamente los datos "últimos 30 días" sin ordenar en memoria. El servicio asegura la unicidad del registro diario.

*  **TTL:** No aplica. Se requiere retención histórica indefinida para auditorías de facturación y comparativas anuales (Year-over-Year).

  

**2. Caso de Uso: Soporte (Tabla `org_daily_support_metrics`)**

*  **Partition Key:**  `org_id`.

*  **Clustering Key:**  `ticket_date` (DESC).

*  *Justificación:* Permite consultas de rango eficientes para un cliente dado.

  

**3. Caso de Uso: Producto/GenAI (Tabla `org_daily_genai_usage`)**

*  **Partition Key:**  `org_id`.

*  **Clustering Keys:**  `event_date` (DESC), `service_name`.

*  *Justificación:* Segrega el tráfico de alto volumen de GenAI para no afectar la lectura de costos generales.
  

### Scripts CQL

A continuación se incluyen todas las *queries* CQL implementadas en `cassandra_utils.py` para la creación de las tablas de FinOps, Soporte y GenAI.


```sql

-- 1. Tabla FinOps: Costos y métricas operativas diarias por servicio

CREATE  TABLE  IF  NOT  EXISTS  "Cloud_analytics_db".org_daily_usage_by_service (

org_id text,

usage_date date,

service_name  text,

daily_cost_usd double,

daily_requests double,

daily_carbon_kg double,

cost_per_request double,

carbon_per_dollar double,

PRIMARY KEY ((org_id), usage_date, service_name)

) WITH CLUSTERING ORDER BY (usage_date DESC, service_name  ASC);

  
-- 2. Tabla de Soporte: Métricas de tickets, SLAs y satisfacción

CREATE  TABLE  IF  NOT  EXISTS  "Cloud_analytics_db".org_daily_support_metrics (

org_id text,

ticket_date date,

total_tickets int,

critical_tickets int,

sla_breached_count int,

avg_csat double,

sla_breach_rate double,

PRIMARY KEY ((org_id), ticket_date)

) WITH CLUSTERING ORDER BY (ticket_date DESC);

-- 3. Tabla de GenAI: Consumo específico de tokens y costos de IA

CREATE  TABLE  IF  NOT  EXISTS  "Cloud_analytics_db".org_daily_genai_usage (

org_id text,

event_date date,

service_name  text,

genai_daily_cost double,

genai_requests_count double,

PRIMARY KEY ((org_id), event_date, service_name)

) WITH CLUSTERING ORDER BY (event_date DESC);
 
```

### Carga a Cassandra

*Evidencia de carga a Cassandra (`cassandra_loader.py` output):*

```python console
[SERVING] Iniciando carga a AstraDB...
[CASSANDRA] Creando esquema en keyspace 'Cloud_analytics_db'...
[CASSANDRA] Esquema verificado: 3 Tablas listas.
   -> cargando tabla 'org_daily_usage_by_service' (731 registros)...

Subiendo org_daily_usage_by_service: 100%

 731/731 [00:15<00:00, 46.48rows/s]

      [OK] Carga completada.
   -> cargando tabla 'org_daily_support_metrics' (944 registros)...

Subiendo org_daily_support_metrics: 100%

 944/944 [00:20<00:00, 46.19rows/s]

      [OK] Carga completada.
   -> cargando tabla 'org_daily_genai_usage' (78 registros)...

Subiendo org_daily_genai_usage: 100%

 78/78 [00:01<00:00, 47.25rows/s]

      [OK] Carga completada.
```

### Chequeo de tablas

```python console
chequeo de carga en Cassandra (Keyspace: Cloud_analytics_db)

Tabla: `org_daily_usage_by_service`
- Total de registros : 876

Muestra de datos:

| org_id       | usage_date   | service_name   |   carbon_per_dollar |   cost_per_request |   daily_carbon_kg |   daily_cost_usd |   daily_requests |
|:-------------|:-------------|:---------------|--------------------:|-------------------:|------------------:|-----------------:|-----------------:|
| org_c11ertj5 | 2025-08-31   | compute        |          0.00326907 |          0.0638383 |          0.025043 |           7.6606 |              120 |
| org_c11ertj5 | 2025-08-31   | database       |          0.00413361 |          0.0504168 |          0.0248   |           5.9996 |              119 |
| org_c11ertj5 | 2025-08-31   | genai          |          0.00157653 |          0.127047  |          0.049072 |          31.1266 |              245 |
| org_c11ertj5 | 2025-08-18   | compute        |          0.00292469 |        nan         |          0.000167 |           0.0571 |                0 |
| org_c11ertj5 | 2025-08-13   | database       |          0.00418779 |        nan         |          0.000446 |           0.1065 |                0 |

---

Tabla: `org_daily_support_metrics`
- Total de registros : 944

Muestra de datos:

| org_id       | ticket_date   |   avg_csat |   critical_tickets |   sla_breach_rate |   sla_breached_count |   total_tickets |
|:-------------|:--------------|-----------:|-------------------:|------------------:|---------------------:|----------------:|
| org_c11ertj5 | 2025-08-30    |          3 |                  0 |                 0 |                    0 |               1 |
| org_c11ertj5 | 2025-08-16    |          3 |                  0 |                 0 |                    0 |               1 |
| org_c11ertj5 | 2025-08-07    |          5 |                  0 |                 1 |                    1 |               1 |
| org_c11ertj5 | 2025-08-03    |          2 |                  0 |                 0 |                    0 |               1 |
| org_c11ertj5 | 2025-07-23    |        nan |                  0 |                 0 |                    0 |               1 |

---

Tabla: `org_daily_genai_usage`
- Total de registros : 92

Muestra de datos:

| org_id       | event_date   | service_name   |   genai_daily_cost |   genai_requests_count |
|:-------------|:-------------|:---------------|-------------------:|-----------------------:|
| org_c11ertj5 | 2025-08-31   | genai          |            31.1266 |                    245 |
| org_c11ertj5 | 2025-08-07   | genai          |             0.2865 |                      0 |
| org_c11ertj5 | 2025-07-21   | genai          |            12.6785 |                    110 |
| org_w3zp08j3 | 2025-08-31   | genai          |             0.3548 |                      0 |
| org_kdgigatj | 2025-07-15   | genai          |             0      |                      0 |

---
```

  

---
  

## Idempotencia y Reprocesos

  

La idempotencia se garantiza mediante el diseño de claves primarias naturales en Cassandra, lo que convierte las inserciones en *Upserts*. Se validó ejecutando el proceso de carga dos veces consecutivas y verificando que los conteos no aumentaran.

  *Evidencia (Salida del chequeo de Idempotencia):*

```python console

Prueba de idempotencia 

[1] Contando registros actuales...
    - org_daily_usage_by_service: 876
    - org_daily_support_metrics: 944
    - org_daily_genai_usage: 92

[2] >>> Re-ejecutando Carga (Gold -> Cassandra)...

[SERVING] Iniciando carga masiva a AstraDB...
[CASSANDRA] Creando esquema en keyspace 'Cloud_analytics_db'...
[CASSANDRA] Esquema verificado: 3 Tablas listas.
   -> cargando tabla 'org_daily_usage_by_service' (727 registros)...

Subiendo org_daily_usage_by_service: 100%

 727/727 [00:26<00:00, 27.93rows/s]

      [OK] Carga completada.
   -> cargando tabla 'org_daily_support_metrics' (944 registros)...

Subiendo org_daily_support_metrics: 100%

 944/944 [00:34<00:00, 28.07rows/s]

      [OK] Carga completada.
   -> cargando tabla 'org_daily_genai_usage' (77 registros)...

Subiendo org_daily_genai_usage: 100%

 77/77 [00:02<00:00, 27.60rows/s]

      [OK] Carga completada.

[3] Verificando consistencia...
    - org_daily_usage_by_service: 876 -> 876 [OK]
    - org_daily_support_metrics: 944 -> 944 [OK]
    - org_daily_genai_usage: 92 -> 92 [OK]
--------------------------------------------------
Resultado: 0 Duplicados generados.
```
Como se puede ver, al realizar upserts los duplicados terminan siendo cero, confirmando la idempotencia en la base de Cassandra

---


## Performance

Para garantizar un rendimiento óptimo en un entorno de recursos limitados (Single-Node en Colab) y minimizar la latencia, se implementaron tres estrategias clave de optimización en Spark:

1.  **Broadcast Hash Join (Map-Side Join):** En la capa Silver, al cruzar la tabla de hechos masiva (`usage_events`) con la dimensión pequeña (`customers_orgs`), se forzo un `F.broadcast()`. Esto evita el intercambio de datos (*Shuffle*) a través de la red, enviando una copia completa de la tabla de clientes a la memoria de cada ejecutor, transformando un *SortMergeJoin* en un *BroadcastHashJoin* mucho más rápido.
2.  **Ajuste de Shuffle Partitions:** Se configuró `spark.sql.shuffle.partitions = 4` (el default es 200). Dado que el volumen de datos del MVP es moderado (inferior a 10 GB), mantener 200 particiones generaría una sobrecarga excesiva en la planificación de tareas (*task scheduling overhead*) y fragmentación de archivos pequeños (*small files*). Al reducirlo a 4, se optimizan las tareas, maximizando el output
3.  **Caching en Streaming:** En la *Speed Layer*, se aplicó `.cache()` a la dimensión de organizaciones antes de iniciar el flujo. Sin esto, Spark volvería a leer los archivos Parquet de la capa Bronze en cada *micro-batch* (cada 5 segundos), degradando la latencia I/O. Al mantener la tabla en memoria RAM, el *lookup* es instantáneo.

**Métricas observadas:**
Gracias a estas optimizaciones, el procesamiento de la capa Silver (limpieza y joins) mantuvoun  tiempo de ejecución de **~12 segundos**. Asimismo, la carga a Cassandra mantuvo una tasa de escritura constante de **~45 filas/segundo**, limitada únicamente por el ancho de banda de red hacia AstraDB y no por el procesamiento de Spark.
 
 ---
  
## Consultas mínimas desde AstraDB

Se armaron las siguientes Queries CQL para realizar uan evaluacion del estado de las tablas subidas a Cassandra yver que tan factible es realizar consultas de negocios reales.
Si bien se pedian 5 queries, se realizó una sexta respecto al rema sustentabilidad para asi mostrar mas  posibiliades de las tablas.

 **Queries**

```python
# (1) FinOps: Costos y requests diarios (rango de fechas)
    print("\n1. [FinOps] Costos y requests diarios (Últimos 7 días)")
    query_1 = f"""
        SELECT usage_date, service_name, daily_cost_usd, daily_requests
        FROM "{KEYSPACE}".org_daily_usage_by_service
        WHERE org_id = '{TARGET_ORG}'
        AND usage_date >= '{ref_date - timedelta(days=7)}'
    """
    rows = session.execute(query_1)
    print(f"{'Fecha':<12} | {'Servicio':<15} | {'Costo ($)':<10} | {'Requests'}")
    for r in rows:
        print(f"{str(r.usage_date):<12} | {r.service_name:<15} | {r.daily_cost_usd:<10.2f} | {int(r.daily_requests)}")


    # (2) FinOps: Top 3 servicios mas costosos (ultimos 14 dias)
    print(f"\n2. [FinOps] Top 3 Servicios más costosos (Desde {date_14_days_ago})")
    query_2 = f"""
        SELECT service_name, daily_cost_usd
        FROM "{KEYSPACE}".org_daily_usage_by_service
        WHERE org_id = '{TARGET_ORG}'
        AND usage_date >= '{date_14_days_ago}'
    """
    rows = session.execute(query_2)

    # agregacion client-side
    cost_map = {}
    for r in rows:
        cost_map[r.service_name] = cost_map.get(r.service_name, 0.0) + r.daily_cost_usd

    sorted_services = sorted(cost_map.items(), key=lambda x: x[1], reverse=True)[:3]

    print(f"{'Servicio':<15} | {'Costo Acum. ($)'}")
    for svc, cost in sorted_services:
        print(f"{svc:<15} | {cost:.2f}")


    # (3) Soporte: Tickets criticos y SLA breach rate (30 dias)
    print(f"\n3. [Soporte] Tickets críticos y Tasa SLA Breach (Desde {date_30_days_ago})")
    query_3 = f"""
        SELECT ticket_date, critical_tickets, sla_breach_rate
        FROM "{KEYSPACE}".org_daily_support_metrics
        WHERE org_id = '{TARGET_ORG}'
        AND ticket_date >= '{date_30_days_ago}'
    """
    rows = session.execute(query_3)

    # manejo por si no hay tickets
    if not rows:
        print("(Sin tickets en este periodo para esta org)")

    print(f"{'Fecha':<12} | {'Críticos':<8} | {'Tasa Breach %'}")
    for r in rows:
        rate_pct = r.sla_breach_rate * 100 if r.sla_breach_rate else 0.0
        print(f"{str(r.ticket_date):<12} | {r.critical_tickets:<8} | {rate_pct:.1f}%")


    # (4) Finanzas: Revenue mensual estimado (con tax)
    # nota: tomo desde usage
    print(f"\n4. [Finanzas] Revenue Mensual Estimado (Mes actual: {start_of_month})")

    query_4 = f"""
        SELECT daily_cost_usd
        FROM "{KEYSPACE}".org_daily_usage_by_service
        WHERE org_id = '{TARGET_ORG}'
        AND usage_date >= '{start_of_month}'
    """
    rows = session.execute(query_4)

    monthly_subtotal = sum([r.daily_cost_usd for r in rows])
    taxes = monthly_subtotal * 0.21 # 21% tax -tome el IVA como ejemplo, eso lo decide finanzas
    credits = 0.0
    total_revenue = monthly_subtotal + taxes - credits

    print(f"Subtotal Uso:    ${monthly_subtotal:.2f}")
    print(f"Impuestos (est): ${taxes:.2f}")
    print(f"TOTAL REVENUE:   ${total_revenue:.2f}")


    # (5) Producto: Consumo GenAI (tokens y costo)
    print("\n5. [Producto] Consumo GenAI (Tokens/Requests y Costo)")
    query_5 = f"""
        SELECT event_date, service_name, genai_requests_count, genai_daily_cost
        FROM "{KEYSPACE}".org_daily_genai_usage
        WHERE org_id = '{TARGET_ORG}'
        LIMIT 5
    """
    rows = list(session.execute(query_5))

    if not rows:
        print("(Esta organización no registra consumo de GenAI)")
    else:
        print(f"{'Fecha':<12} | {'Servicio':<12} | {'Tokens/Reqs':<12} | {'Costo ($)'}")
        for r in rows:
            print(f"{str(r.event_date):<12} | {r.service_name:<12} | {int(r.genai_requests_count):<12} | {r.genai_daily_cost:.2f}")

  # (6) Sustentabilidad
    print("\n6. [ESG] Huella de Carbono")
    query_4 = f"""
        SELECT usage_date, service_name, daily_carbon_kg
        FROM "{KEYSPACE}".org_daily_usage_by_service
        WHERE org_id = '{TARGET_ORG}'
        LIMIT 5
    """
    rows = session.execute(query_4)
    print(f"   {'Fecha':<12} | {'Servicio':<15} | {'Carbono (kgCO2)'}")
    print("   " + "-"*50)
    for r in rows:
        print(f"   {str(r.usage_date):<12} | {r.service_name:<15} | {f'{r.daily_carbon_kg:.4f}' if r.daily_carbon_kg is not None else 'None':<15}")
```
### Resultados de las queries - output de celda  `cassandra_queries.py`:

_Observación_ : la celda toma una organizacion random y genera las tablas.
```python console
Queries de negocios 

Organizacion: org_pja1wj0t
Fecha de Referencia: 2025-08-31
------------------------------------------------------------

1. [FinOps] Costos y requests diarios (Últimos 7 días)
Fecha        | Servicio        | Costo ($)  | Requests
2025-08-31   | compute         | 11.93      | 111
2025-08-31   | networking      | 2.51       | 234
2025-08-27   | compute         | 0.11       | 0

2. [FinOps] Top 3 Servicios más costosos (Desde 2025-08-17)
Servicio        | Costo Acum. ($)
compute         | 12.04
networking      | 2.51

3. [Soporte] Tickets críticos y Tasa SLA Breach (Desde 2025-08-01)
Fecha        | Críticos | Tasa Breach %
2025-08-23   | 0        | 100.0%
2025-08-22   | 0        | 0.0%
2025-08-20   | 0        | 0.0%

4. [Finanzas] Revenue Mensual Estimado (Mes actual: 2025-08-01)
Subtotal Uso:    $14.56
Impuestos (est): $3.06
TOTAL REVENUE:   $17.61

5. [Producto] Consumo GenAI (Tokens/Requests y Costo)
(Esta organización no registra consumo de GenAI)

6. [ESG] Huella de Carbono
   Fecha        | Servicio        | Carbono (kgCO2)
   --------------------------------------------------
   2025-08-31   | compute         | 0.0255         
   2025-08-31   | networking      | 0.0468         
   2025-08-27   | compute         | 0.0002         
   2025-08-05   | networking      | 0.0001         
   2025-07-24   | compute         | 0.0005

```


## Limitaciones principales:

-   El cálculo de Revenue se realiza en tiempo de consulta (On-the-fly) en lugar de pre-agregarse en un Mart físico, lo cual es aceptable para el volumen actual pero no escala bien
    
-   La detección de anomalías se reduce a un umbral simple sobre `cost_usd_increment` y a flags implícitos de calidad; no se utilizaron los tres métodos estadísticos sugeridos (z-score, MAD, percentiles) a los fines de simplificar la entrega.
    
-   La **gestión de `schema_version`** se limita a conservar la columna y explotar `carbon_kg` cuando existe; no hay lógica diferenciada v1/v2 ni backfill explícito para campos nuevos.
    
-  El pipeline tolera cambios de esquema (columna `schema_version`), pero no implementa lógica de backfilling automático ni tablas SCD Tipo 2 para cambios en dimensiones lentas (Organizaciones/Recursos).
    
-   La métricas de GenAI agregan `requests` como proxy de tokens; el agregado explícito de `genai_tokens` queda pendiente

-   Debido a las restricciones del entorno de Google Colab, la orquestación de tareas Batch y Streaming se simuló secuencialmente en un notebook. Un entorno productivo requeriría Cron/Apache Airflow para gestión de dependencias y reintentos.



## Trabajo futuro propuesto:

-   Implementar  un Hardening de Calidad en la capa Silver para incluir validaciones estadísticas (desviación estándar de costos) antes de la carga a Gold. en la capa Silver para incluir validaciones estadísticas (desviación estándar de costos) antes de la carga a Gold.

- Materializar la tabla de Revenue con contadores distribuidos para evitar el costo de agregación en lectura.

- Incorporar z-score, MAD y percentiles para flags de anomalía y consolidarlos en un mart consumible por FinOps.

- Implementar lógica específica por `schema_version` (v1/v2) y, si fuera necesario, tablas SCD para organizaciones/recursos.    

-   Migrar la ejecución a un orquestador (Airflow/cron + Prometheus/Grafana) y conectar directamente AstraDB a Power BI/Looker para dashboards productivos.

- Definir políticas de Backup y Disaster Recovery para el Data Lake y AstraDB. 

- Implementar cifrado en reposo/tránsito y controles de acceso (RBAC) para garantizar el cumplimiento de normativas (GDPR/SOC2) antes del despliegue productivo.# Proyecto “Cloud Provider Analytics” – Final

  

>  **Formato:** Documento oficial de entrega del MVP final.

  

---

  


-  **Título del proyecto:** Cloud Provider Analytics: Plataforma de Inteligencia de Costos y Operaciones

-  **Integrantes:** [Completar con Nombres]

  
  

## Resumen Ejecutivo

  
Este proyecto  implementa un ecosistema analítico para un proveedor de nube ficticio que necesita integrar datos de **FinOps, Soporte y Producto/GenAI** a partir de fuentes heterogéneas y con evolución de esquema (`schema_version`, aparición de `carbon_kg` y `genai_tokens`) para habilitar analítica diaria y _near real-time_.

Se adopta un arquitectura de solución tipo **Lambda**: pipeline batch para maestros (CRM, facturación, NPS) y pipeline de _Structured Streaming_ para eventos de uso (`usage_events_stream`) sobre un data lake Parquet (Landing → Bronze → Silver → Gold), con publicación final de _marts_ en **AstraDB (Cassandra)** para consumo por herramientas de BI.

El MVP final implementa: 
1.  ingesta batch de CSV a Bronze con esquemas tipados; 
2.  _streaming_ de eventos JSONL con `withWatermark` + `dropDuplicates(event_id)`;
3.  limpieza y conformidad en Silver, incluyendo integración con dimensiones de organizaciones y recursos; 
4. _marts_ Gold para FinOps (`org_daily_usage_by_service`), Soporte (`org_daily_support_metrics`) y GenAI (`org_daily_genai_usage`); 
5.  carga idempotente a Cassandra con driver Python y pruebas explícitas de reproceso; 
6.  consultas de negocio desde AstraDB 

El resultado es un pipeline *end-to-end* que alimenta una *Serving Layer* en AstraDB (Cassandra), permitiendo consultas de alto rendimiento. Los usuarios pueden obtener *insights* inmediatos, como identificar los servicios más costosos de la última semana o correlacionar picos de incidentes críticos con fechas de despliegue, todo validado mediante un dashboard interactivo.


---

  

## Arquitectura Final

  
**1. Diagrama de alto nivel**

El flujo implementa una arquitectura Lambda clásica. Los datos CSV (Maestros) siguen la ruta Batch (Landing $\rightarrow$ Bronze $\rightarrow$ Silver $\rightarrow$ Gold), mientras que los eventos JSON siguen la ruta Speed (Streaming $\rightarrow$ Gold Directo). Ambas rutas convergen en la capa de servicio (Cassandra).

  

![Diagrama de flujo de arquitectura Lambda](https://github.com/Sinnick4r/Cloud_Provider_Analytics_MVP/blob/main/documentacion/Diagrama%20Proyecto.png?raw=true)

  

Se eligió un patrón de **Arquitectura Lambda** para combinar la robustez del procesamiento *Batch* (necesario para la conciliación precisa de facturación histórica y datos maestros) con la baja latencia del *Speed Layer* (crítico para alertas de consumo en tiempo real), permitiendo responder preguntas de negocio complejas sobre costos y operaciones.


**2. Patrón elegido y Decisiones clave**

*  **Particionamiento:**  `hq_region` para maestros estáticos y `event_date` para eventos transaccionales, optimizando la lectura.

*  **Formatos:** Parquet en todo el Data Lake por eficiencia de I/O y soporte de esquemas.

  
**3. Componentes y dependencias**

*  **Procesamiento:** Apache Spark 3.5.1 (PySpark).

*  **Storage:** Sistema de archivos local simulando S3/HDFS.

*  **Serving:** DataStax AstraDB (Cassandra) + Driver Python.

*  **Dependencias:**  `pyspark`, `cassandra-driver`, `astrapy`, `gradio`, `plotly`.

  ---

  

## Datos y Supuestos

  
Los datos provienen de un entorno simulado de proveedor de nube. Se normalizaron timestamps a UTC y montos a decimales. A continuación, el diccionario de datos completo basado en los esquemas definidos en `schemas.py`.

  

**Diccionario de datos clave (extracto):**

  

| Campo | Dataset | Tipo | Descripción | Observaciones |
|---|---|---|---|---|
| `org_id` | customers_orgs | String | ID único de la organización | Clave primaria y de partición en Cassandra. |
| `hq_region` | customers_orgs | String | Región de la sede | Usado para particionamiento en Bronze. |
| `plan_tier` | customers_orgs | String | Nivel de plan (Free, Pro, Enterprise) | Usado para segmentación en Silver. |
| `user_id` | users | String | ID único del usuario | - |
| `role` | users | String | Rol del usuario (Admin, Viewer) | - |
| `resource_id` | resources | String | ID del recurso en la nube | - |
| `service` | resources | String | Nombre del servicio (compute, storage) | - |
| `ticket_id` | support_tickets | String | ID del ticket de soporte | - |
| `severity` | support_tickets | String | Criticidad (Low, Medium, Critical) | Usado para métricas de SLA. |
| `sla_breached` | support_tickets | Boolean | Flag de incumplimiento de SLA | True si se venció el plazo. |
| `invoice_id` | billing_monthly | String | ID de la factura mensual | - |
| `subtotal` | billing_monthly | Decimal | Monto antes de impuestos | - |
| `event_id` | usage_events | String | ID único del evento de uso | Clave para deduplicación. |
| `cost_usd_increment` | usage_events | Decimal | Costo incremental del evento | Regla de calidad: $\ge -0.01$. |
| `carbon_kg` | usage_events | Double | Huella de carbono estimada | Métrica ESG. |
| `genai_tokens` | usage_events | Long | Tokens consumidos (si aplica) | Métrica específica GenAI (derivada). |

  
---

## Data Lake: Zonas y Particionado

  
El Data Lake se organiza estrictamente en zonas para garantizar la gobernanza y calidad del dato:

*  **Landing:** Datos crudos inmutables (CSV/JSON).

*  **Bronze:** Raw estándar, mismo grano que la fuente, tipificación explícita, dedupe por `event_id` y columnas técnicas `ingest_ts`, `source_file`

*  **Silver:** datos limpios y conformados con joins a dimensiones y tratamiento de nulos/outliers . Particionado por `event_date`.

*  **Gold:** Marts de negocio agregados. Particionado por `event_date` o `ticket_date`.

*  **Quarantine:** Zona aislada para registros que no cumplen reglas de calidad (ej. costos negativos).
  

![Diagrama de Pipelines de Datos](https://github.com/Sinnick4r/Cloud_Provider_Analytics_MVP/blob/main/documentacion/Pipelines.png?raw=true)

###  Directorios del datalake
  ![Diagrama de Pipelines de Datos](https://github.com/Sinnick4r/Cloud_Provider_Analytics_MVP/blob/main/documentacion/estructura%20de%20directorios.png?raw=true)

---

  

## Ingesta y Calidad de Datos

  

### Ingesta Batch

Se implementaron funciones dedicadas para cada dataset maestro, aplicando lectura con esquema estricto, adición de columnas de auditoría y escritura particionada en formato Parquet.

  

*Fragmento de código (`bronze_batch.py`):*

```python

def  ingest_customers_orgs_to_bronze(spark: SparkSession) -> None:

df = _read_landing_csv(spark, "customers_orgs.csv", customers_orgs_schema)

if df is  None: return

df = add_audit_columns(df)

dest = zone_path(BRONZE_PATH, "customers_orgs")

write_parquet(df, dest, partition_cols=["hq_region"])

  

def  ingest_users_to_bronze(spark: SparkSession) -> None:

df = _read_landing_csv(spark, "users.csv", users_schema)

if df is  None: return

df = add_audit_columns(df)

dest = zone_path(BRONZE_PATH, "users")

write_parquet(df, dest, partition_cols=["role"])

  

def  ingest_resources_to_bronze(spark: SparkSession) -> None:

df = _read_landing_csv(spark, "resources.csv", resources_schema)

if df is  None: return

df = add_audit_columns(df)

dest = zone_path(BRONZE_PATH, "resources")

write_parquet(df, dest, partition_cols=["region"])

  

def  ingest_support_tickets_to_bronze(spark: SparkSession) -> None:

df = _read_landing_csv(spark, "support_tickets.csv", support_tickets_schema)

if df is  None: return

df = add_audit_columns(df)

dest = zone_path(BRONZE_PATH, "support_tickets")

write_parquet(df, dest, partition_cols=["severity"])

  

def  ingest_marketing_touches_to_bronze(spark: SparkSession) -> None:

df = _read_landing_csv(spark, "marketing_touches.csv", marketing_touches_schema)

if df is  None: return

df = add_audit_columns(df)

dest = zone_path(BRONZE_PATH, "marketing_touches")

write_parquet(df, dest, partition_cols=["channel"])

  

def  ingest_nps_surveys_to_bronze(spark: SparkSession) -> None:

df = _read_landing_csv(spark, "nps_surveys.csv", nps_surveys_schema)

if df is  None: return

df = add_audit_columns(df)

dest = zone_path(BRONZE_PATH, "nps_surveys")

write_parquet(df, dest, partition_cols=["survey_date"])

  

def  ingest_billing_monthly_to_bronze(spark: SparkSession) -> None:

df = _read_landing_csv(spark, "billing_monthly.csv", billing_monthly_schema)

if df is  None: return

df = add_audit_columns(df)

dest = zone_path(BRONZE_PATH, "billing_monthly")

write_parquet(df, dest, partition_cols=["month"])

```

  

### Ingesta Streaming

Lectura de JSONs en streaming con transformación de fechas, aplicación de *watermarking* de 1 día y deduplicación por `event_id` para garantizar integridad.

  

*Fragmento de código (`bronze_stream.py`):*

```python

def  start_usage_events_to_bronze(spark: SparkSession):

df_stream = create_usage_events_stream(spark)

# Transformaciones: timestamp -> event_ts, watermark, dedupe

df_bronze = transform_usage_events_bronze(df_stream)

  

dest_path = zone_path(BRONZE_PATH, "usage_events")

checkpoint_path = BRONZE_PATH / "_checkpoints" / "usage_events"

query = (

df_bronze

.writeStream

.format("parquet")

.option("checkpointLocation", str(checkpoint_path))

.option("path", str(dest_path))

.partitionBy("event_date")

.outputMode("append")

.start()

)

return query
```
### Resultado de la ingesta Streaming:

```python console
[INFO] Comenzando Speed Layer...
[INFO] Dimensión Organizaciones cacheada: 80 registros.
[INFO] Streaming ejecutandose -> /content/datalake/gold/org_daily_usage_by_service_speed
Streaming Speed → Gold
ID: a3016673-1db2-49d6-a7f8-27d726d023e7
Nombre: None
Activo: True
[STREAM 0] Reporte 
 Input: 360 eventos
Validos: 359
Dropped (Cost < -0.01): 1 (0.3%)
todo pasado a Gold de Speed layer
[STREAM 1] Reporte 
 Input: 360 eventos
Validos: 358
Dropped (Cost < -0.01): 2 (0.6%)
todo pasado a Gold de Speed layer
[STREAM 2] Reporte 
 Input: 11 eventos
Validos: 11
todo pasado a Gold de Speed layer
[STREAM 3] Reporte 
 Input: 7 eventos
Validos: 7
todo pasado a Gold de Speed layer
 Progreso del streaming
{'id': 'a3016673-1db2-49d6-a7f8-27d726d023e7', 'runId': '7f48b7be-d5b7-47dc-b5c2-63ed1b19bef1', 'name': None, 'timestamp': '2025-12-07T19:21:25.000Z', 'batchId': 3, 'numInputRows': 360, 'inputRowsPerSecond': 150.9433962264151, 'processedRowsPerSecond': 173.49397590361446, 'durationMs': {'addBatch': 1863, 'commitOffsets': 56, 'getBatch': 11, 'latestOffset': 54, 'queryPlanning': 32, 'triggerExecution': 2075, 'walCommit': 54}, 'eventTime': {'avg': '2025-08-02T03:04:24.833Z', 'max': '2025-08-31T23:51:00.000Z', 'min': '2025-07-03T00:48:00.000Z', 'watermark': '2025-08-30T23:51:00.000Z'}, 'stateOperators': [{'operatorName': 'dedupe', 'numRowsTotal': 2410, 'numRowsUpdated': 25, 'allUpdatesTimeMs': 194, 'numRowsRemoved': 0, 'allRemovalsTimeMs': 0, 'commitTimeMs': 969, 'memoryUsedBytes': 481296, 'numRowsDroppedByWatermark': 1142, 'numShufflePartitions': 13, 'numStateStoreInstances': 13, 'customMetrics': {'loadedMapCacheHitCount': 74, 'loadedMapCacheMissCount': 40, 'numDroppedDuplicateRows': 0, 'stateOnCurrentVersionSizeBytes': 445792}}], 'sources': [{'description': 'FileStreamSource[file:/content/datalake/landing/usage_events_stream]', 'startOffset': {'logOffset': 2}, 'endOffset': {'logOffset': 3}, 'latestOffset': None, 'numInputRows': 360, 'inputRowsPerSecond': 150.9433962264151, 'processedRowsPerSecond': 173.49397590361446}], 'sink': {'description': 'ForeachBatchSink', 'numOutputRows': -1}}
aparando stream...

 cheque de Speed Layer ---
Ruta: /content/datalake/gold/org_daily_usage_by_service_speed, Total acumulado en disco: 727
funcionando todo OK (Datos persistidos correctamente)
+------------+---------------+------------+--------------+--------------+---------------+----------+
|org_id      |org_name       |service_name|daily_cost_usd|daily_requests|daily_carbon_kg|event_date|
+------------+---------------+------------+--------------+--------------+---------------+----------+
|org_pnsm43d8|Delta Labs 70  |compute     |11.6645       |130.0000      |0.026          |2025-08-13|
|org_dhylurtp|Nimbus Cloud 76|compute     |11.9243       |133.0000      |0.0266         |2025-08-13|
|org_5iqvnb4g|Gamma Labs 73  |networking  |0.0092        |0.0000        |2.05E-4        |2025-08-13|
+------------+---------------+------------+--------------+--------------+---------------+----------+
only showing top 3 rows

Streaming Speed → Gold parado.
```

  

### 5.3 Reglas de Calidad y Evidencias

Se verifica unicidad de claves primarias y completitud de campos de auditoría. En una unica celda se chequeo la integridad de todo lo realizado en las capas Bronze, Silver y Gold.

*Salida de ejecución (`audit.py`):*

```python console

[2025-12-07 19:20:51] [RUN] Haciendo quality control...  [2025-12-07 19:20:51] [RUN] Iniciando Auditoría completa de capa Bronze... chequeo Bronze: customers_orgs
registros totales: 80
duplicados en PK (org_id): 0
nulos en ingest_ts: 0
Resultado: todo ok!!

 chequeo Bronze: users
registros totales: 800
duplicados en PK (user_id): 0
nulos en ingest_ts: 0
Resultado: todo ok!!

 chequeo Bronze: resources
registros totales: 400
duplicados en PK (resource_id): 0
nulos en ingest_ts: 0
Resultado: todo ok!!

 chequeo Bronze: support_tickets
registros totales: 1000
duplicados en PK (ticket_id): 0
nulos en ingest_ts: 0
Resultado: todo ok!!

 chequeo Bronze: marketing_touches
registros totales: 1500
duplicados en PK (touch_id): 0
nulos en ingest_ts: 0
Resultado: todo ok!!

 chequeo Bronze: billing_monthly
registros totales: 240
duplicados en PK (invoice_id): 0
nulos en ingest_ts: 0
Resultado: todo ok!!

 chequeo Bronze: nps_surveys
registros totales: 92
duplicados en PK (org_id): 32
nulos en ingest_ts: 0
Resultado: revisar data por posibles duplicados [2025-12-07 19:20:58] [OK] Auditoría Bronze finalizada.  [2025-12-07 19:20:58] [RUN] Iniciando Auditoría de Calidad Silver... Chequeop Silver:
Total: 758
Aceptados (Silver): 755
Rechazados (cuarentena): 3 (0.40%)
CALIDAD: ACEPTABLE
ejemplo de rechazo:
+----------------+-------------------------+
|event_id        |quarantine_reason        |
+----------------+-------------------------+
|evt_faubjbtabmwl|cost_negative_or_null_org|
+----------------+-------------------------+
only showing top 1 row


Calidad de Datos (Silver Batch)

 Estadisticas:
Total Procesado: 758
Aceptados (Silver): 755 (99.60%)
Rechazados (Quarantine): 3 (0.40%)

resultado:
Aceptable- rechazo  bajo y esperado.

 Muestra de registros en Cuarentena (top 5):
+----------------+------------------+------------+-------------------------+
|event_id        |cost_usd_increment|org_id      |quarantine_reason        |
+----------------+------------------+------------+-------------------------+
|evt_faubjbtabmwl|-0.0602           |org_cvs4f8cg|cost_negative_or_null_org|
|evt_qniow8ymxwd6|-0.2446           |org_i7p5tb94|cost_negative_or_null_org|
|evt_bbmth9hzpa6e|-0.3656           |org_n9j2qp89|cost_negative_or_null_org|
+----------------+------------------+------------+-------------------------+ [2025-12-07 19:21:06] [OK] Auditoría Silver finalizada.  [2025-12-07 19:21:06] [RUN] Iniciando Auditoría de Marts Gold... Chequeo Gold: org_daily_usage_by_service
registros Totales (Agregados): 727
Costos Negativos detectados: 0

 Chequeo Gold: org_daily_support_metrics
registros Totales (Agregados): 944
Costos Negativos detectados: 0

 Chequeo Gold: org_daily_genai_usage
registros Totales (Agregados): 77
Costos Negativos detectados: 0 [2025-12-07 19:21:11] [OK] Auditoría Gold finalizada.  [2025-12-07 19:21:11] [OK] Fin demo Batch Layer (Bronze CSV → Silver → Gold)

```  

---

  

## Transformaciones ( Etapa Silver)

  

En la capa Silver (`run_silver_batch`), se realizan transformaciones críticas de limpieza y enriquecimiento. Se utiliza un **Broadcast Join** para cruzar los eventos masivos con los datos de organizaciones.

  

**Cálculos realizados:**

*  **Suma de Costos:** Agregación de `cost_usd_increment` como `daily_cost_usd`.

*  **Conteo de Requests:** Suma condicional donde `metric == 'requests'`.

*  **Cálculo de Carbono:** Acumulación directa de `carbon_kg`.

*  **KPIs Derivados:**  `cost_per_request` y `carbon_per_dollar`.

*  **Soporte:** Conteo de `sla_breached` para obtener la tasa de incumplimiento (`sla_breach_rate`).

  

*Fragmento de código (Reglas de Calidad y Cuarentena):*

```python

# Reglas: Costo >= -0.01 y org_id no nulo

dq_condition = (F.col("cost_usd_increment") >= -0.01) & (F.col("org_id").isNotNull())

good_df = enriched_df.filter(dq_condition)

bad_df = enriched_df.filter(~dq_condition)

  

# Logica Left Anti para evitar duplicados en cuarentena

existing_quarantine = read_parquet(spark, quarantine_dest)

unique_bad_df = bad_df.join(existing_quarantine, on="event_id", how="left_anti")

```

  ### Resultado de la ingesta + Silver a gold (celda demo)
```python console
[OK] Dataset descomprimido en /content/datalake/landing
[2025-12-07 19:19:40] [INFO] PROJECT_ROOT: {PROJECT_ROOT}  [2025-12-07 19:19:40] [INFO] Spark version: 3.5.1  [2025-12-07 19:19:40] [RUN] Generando Bronze Batch (Maestros)... [OK] Dataset descomprimido en /content/datalake/landing

[BATCH] Iniciando Ingesta a Bronze (7 Maestros)... 

Procesando Archivos: 100%

 7/7 [00:19<00:00,  1.83s/tablas]

[INFO] Leyendo /content/datalake/landing/customers_orgs.csv
[OK] Bronze customers_orgs -> /content/datalake/bronze/customers_orgs
[INFO] Leyendo /content/datalake/landing/users.csv
[OK] Bronze users -> /content/datalake/bronze/users
[INFO] Leyendo /content/datalake/landing/resources.csv
[OK] Bronze resources -> /content/datalake/bronze/resources
[INFO] Leyendo /content/datalake/landing/support_tickets.csv
[OK] Bronze support_tickets -> /content/datalake/bronze/support_tickets
[INFO] Leyendo /content/datalake/landing/marketing_touches.csv
[OK] Bronze marketing_touches -> /content/datalake/bronze/marketing_touches
[INFO] Leyendo /content/datalake/landing/nps_surveys.csv
[OK] Bronze nps_surveys -> /content/datalake/bronze/nps_surveys
[INFO] Leyendo /content/datalake/landing/billing_monthly.csv
[OK] Bronze billing_monthly -> /content/datalake/bronze/billing_monthly
[2025-12-07 19:20:03] [OK] Capa Bronze finalizada correctamente  [2025-12-07 19:20:03] [RUN] Generando Bronze Stream (Eventos)... [INFO] Streaming usage_events -> /content/datalake/bronze/usage_events
[INFO] Checkpoints en /content/datalake/bronze/_checkpoints/usage_events [2025-12-07 19:20:19] [OK] Stream procesado.  [2025-12-07 19:20:19] [RUN] Generando Silver... [INFO] Iniciando Silver...
[WARN] Creando cuarentena por primera vez
[OK] Silver Batch completado, todo ok -> /content/datalake/silver/usage_events_enriched [2025-12-07 19:20:38] [OK] Silver finalizado  [2025-12-07 19:20:38] [RUN] ejecuntando proceso Gold... [OK] Gold Batch (FinOps) -> /content/datalake/gold/org_daily_usage_by_service
[OK] Gold Batch (Support) -> /content/datalake/gold/org_daily_support_metrics
[OK] Gold Batch (GenAI) -> /content/datalake/gold/org_daily_genai_usage [2025-12-07 19:20:51] [OK] proceso Gold finalizado
```


---

  

## Modelado Gold y Serving en AstraDB (Cassandra)

  

### Diseño por consulta (query-first)

Se diseñaron tablas con claves compuestas (`org_id` como Partition Key) para satisfacer consultas de dashboards por cliente y rango de fechas.  

El modelo de datos se diseñó siguiendo la metodología *Query-First* de Cassandra, optimizando las tablas para los filtros específicos de los dashboards de negocio.

  

**1. Caso de Uso: FinOps (Tabla `org_daily_usage_by_service`)**

*  **Partition Key:**  `org_id`.

*  *Justificación:* Los dashboards siempre filtran por un cliente específico. Esto asegura que todos los datos de una organización residan en el mismo nodo, maximizando la velocidad de lectura.

*  **Clustering Keys:**  `usage_date` (DESC), `service_name` (ASC).

*  *Justificación:* El ordenamiento por fecha descendente permite obtener rápidamente los datos "últimos 30 días" sin ordenar en memoria. El servicio asegura la unicidad del registro diario.

*  **TTL:** No aplica. Se requiere retención histórica indefinida para auditorías de facturación y comparativas anuales (Year-over-Year).

  

**2. Caso de Uso: Soporte (Tabla `org_daily_support_metrics`)**

*  **Partition Key:**  `org_id`.

*  **Clustering Key:**  `ticket_date` (DESC).

*  *Justificación:* Permite consultas de rango eficientes para un cliente dado.

  

**3. Caso de Uso: Producto/GenAI (Tabla `org_daily_genai_usage`)**

*  **Partition Key:**  `org_id`.

*  **Clustering Keys:**  `event_date` (DESC), `service_name`.

*  *Justificación:* Segrega el tráfico de alto volumen de GenAI para no afectar la lectura de costos generales.
  

### Scripts CQL

A continuación se incluyen todas las *queries* CQL implementadas en `cassandra_utils.py` para la creación de las tablas de FinOps, Soporte y GenAI.


```sql

-- 1. Tabla FinOps: Costos y métricas operativas diarias por servicio

CREATE  TABLE  IF  NOT  EXISTS  "Cloud_analytics_db".org_daily_usage_by_service (

org_id text,

usage_date date,

service_name  text,

daily_cost_usd double,

daily_requests double,

daily_carbon_kg double,

cost_per_request double,

carbon_per_dollar double,

PRIMARY KEY ((org_id), usage_date, service_name)

) WITH CLUSTERING ORDER BY (usage_date DESC, service_name  ASC);

  
-- 2. Tabla de Soporte: Métricas de tickets, SLAs y satisfacción

CREATE  TABLE  IF  NOT  EXISTS  "Cloud_analytics_db".org_daily_support_metrics (

org_id text,

ticket_date date,

total_tickets int,

critical_tickets int,

sla_breached_count int,

avg_csat double,

sla_breach_rate double,

PRIMARY KEY ((org_id), ticket_date)

) WITH CLUSTERING ORDER BY (ticket_date DESC);

-- 3. Tabla de GenAI: Consumo específico de tokens y costos de IA

CREATE  TABLE  IF  NOT  EXISTS  "Cloud_analytics_db".org_daily_genai_usage (

org_id text,

event_date date,

service_name  text,

genai_daily_cost double,

genai_requests_count double,

PRIMARY KEY ((org_id), event_date, service_name)

) WITH CLUSTERING ORDER BY (event_date DESC);
 
```

### Carga a Cassandra

*Evidencia de carga a Cassandra (`cassandra_loader.py` output):*

```python console
[SERVING] Iniciando carga a AstraDB...
[CASSANDRA] Creando esquema en keyspace 'Cloud_analytics_db'...
[CASSANDRA] Esquema verificado: 3 Tablas listas.
   -> cargando tabla 'org_daily_usage_by_service' (731 registros)...

Subiendo org_daily_usage_by_service: 100%

 731/731 [00:15<00:00, 46.48rows/s]

      [OK] Carga completada.
   -> cargando tabla 'org_daily_support_metrics' (944 registros)...

Subiendo org_daily_support_metrics: 100%

 944/944 [00:20<00:00, 46.19rows/s]

      [OK] Carga completada.
   -> cargando tabla 'org_daily_genai_usage' (78 registros)...

Subiendo org_daily_genai_usage: 100%

 78/78 [00:01<00:00, 47.25rows/s]

      [OK] Carga completada.
```

### Chequeo de tablas

```python console
chequeo de carga en Cassandra (Keyspace: Cloud_analytics_db)

Tabla: `org_daily_usage_by_service`
- Total de registros : 876

Muestra de datos:

| org_id       | usage_date   | service_name   |   carbon_per_dollar |   cost_per_request |   daily_carbon_kg |   daily_cost_usd |   daily_requests |
|:-------------|:-------------|:---------------|--------------------:|-------------------:|------------------:|-----------------:|-----------------:|
| org_c11ertj5 | 2025-08-31   | compute        |          0.00326907 |          0.0638383 |          0.025043 |           7.6606 |              120 |
| org_c11ertj5 | 2025-08-31   | database       |          0.00413361 |          0.0504168 |          0.0248   |           5.9996 |              119 |
| org_c11ertj5 | 2025-08-31   | genai          |          0.00157653 |          0.127047  |          0.049072 |          31.1266 |              245 |
| org_c11ertj5 | 2025-08-18   | compute        |          0.00292469 |        nan         |          0.000167 |           0.0571 |                0 |
| org_c11ertj5 | 2025-08-13   | database       |          0.00418779 |        nan         |          0.000446 |           0.1065 |                0 |

---

Tabla: `org_daily_support_metrics`
- Total de registros : 944

Muestra de datos:

| org_id       | ticket_date   |   avg_csat |   critical_tickets |   sla_breach_rate |   sla_breached_count |   total_tickets |
|:-------------|:--------------|-----------:|-------------------:|------------------:|---------------------:|----------------:|
| org_c11ertj5 | 2025-08-30    |          3 |                  0 |                 0 |                    0 |               1 |
| org_c11ertj5 | 2025-08-16    |          3 |                  0 |                 0 |                    0 |               1 |
| org_c11ertj5 | 2025-08-07    |          5 |                  0 |                 1 |                    1 |               1 |
| org_c11ertj5 | 2025-08-03    |          2 |                  0 |                 0 |                    0 |               1 |
| org_c11ertj5 | 2025-07-23    |        nan |                  0 |                 0 |                    0 |               1 |

---

Tabla: `org_daily_genai_usage`
- Total de registros : 92

Muestra de datos:

| org_id       | event_date   | service_name   |   genai_daily_cost |   genai_requests_count |
|:-------------|:-------------|:---------------|-------------------:|-----------------------:|
| org_c11ertj5 | 2025-08-31   | genai          |            31.1266 |                    245 |
| org_c11ertj5 | 2025-08-07   | genai          |             0.2865 |                      0 |
| org_c11ertj5 | 2025-07-21   | genai          |            12.6785 |                    110 |
| org_w3zp08j3 | 2025-08-31   | genai          |             0.3548 |                      0 |
| org_kdgigatj | 2025-07-15   | genai          |             0      |                      0 |

---
```

  

---
  

## Idempotencia y Reprocesos

  

La idempotencia se garantiza mediante el diseño de claves primarias naturales en Cassandra, lo que convierte las inserciones en *Upserts*. Se validó ejecutando el proceso de carga dos veces consecutivas y verificando que los conteos no aumentaran.

  *Evidencia (Salida del chequeo de Idempotencia):*

```python console

Prueba de idempotencia 

[1] Contando registros actuales...
    - org_daily_usage_by_service: 876
    - org_daily_support_metrics: 944
    - org_daily_genai_usage: 92

[2] >>> Re-ejecutando Carga (Gold -> Cassandra)...

[SERVING] Iniciando carga masiva a AstraDB...
[CASSANDRA] Creando esquema en keyspace 'Cloud_analytics_db'...
[CASSANDRA] Esquema verificado: 3 Tablas listas.
   -> cargando tabla 'org_daily_usage_by_service' (727 registros)...

Subiendo org_daily_usage_by_service: 100%

 727/727 [00:26<00:00, 27.93rows/s]

      [OK] Carga completada.
   -> cargando tabla 'org_daily_support_metrics' (944 registros)...

Subiendo org_daily_support_metrics: 100%

 944/944 [00:34<00:00, 28.07rows/s]

      [OK] Carga completada.
   -> cargando tabla 'org_daily_genai_usage' (77 registros)...

Subiendo org_daily_genai_usage: 100%

 77/77 [00:02<00:00, 27.60rows/s]

      [OK] Carga completada.

[3] Verificando consistencia...
    - org_daily_usage_by_service: 876 -> 876 [OK]
    - org_daily_support_metrics: 944 -> 944 [OK]
    - org_daily_genai_usage: 92 -> 92 [OK]
--------------------------------------------------
Resultado: 0 Duplicados generados.
```
Como se puede ver, al realizar upserts los duplicados terminan siendo cero, confirmando la idempotencia en la base de Cassandra

---


## Performance

Para garantizar un rendimiento óptimo en un entorno de recursos limitados (Single-Node en Colab) y minimizar la latencia, se implementaron tres estrategias clave de optimización en Spark:

1.  **Broadcast Hash Join (Map-Side Join):** En la capa Silver, al cruzar la tabla de hechos masiva (`usage_events`) con la dimensión pequeña (`customers_orgs`), se forzo un `F.broadcast()`. Esto evita el intercambio de datos (*Shuffle*) a través de la red, enviando una copia completa de la tabla de clientes a la memoria de cada ejecutor, transformando un *SortMergeJoin* en un *BroadcastHashJoin* mucho más rápido.
2.  **Ajuste de Shuffle Partitions:** Se configuró `spark.sql.shuffle.partitions = 4` (el default es 200). Dado que el volumen de datos del MVP es moderado (inferior a 10 GB), mantener 200 particiones generaría una sobrecarga excesiva en la planificación de tareas (*task scheduling overhead*) y fragmentación de archivos pequeños (*small files*). Al reducirlo a 4, se optimizan las tareas, maximizando el output
3.  **Caching en Streaming:** En la *Speed Layer*, se aplicó `.cache()` a la dimensión de organizaciones antes de iniciar el flujo. Sin esto, Spark volvería a leer los archivos Parquet de la capa Bronze en cada *micro-batch* (cada 5 segundos), degradando la latencia I/O. Al mantener la tabla en memoria RAM, el *lookup* es instantáneo.

**Métricas observadas:**
Gracias a estas optimizaciones, el procesamiento de la capa Silver (limpieza y joins) mantuvoun  tiempo de ejecución de **~12 segundos**. Asimismo, la carga a Cassandra mantuvo una tasa de escritura constante de **~45 filas/segundo**, limitada únicamente por el ancho de banda de red hacia AstraDB y no por el procesamiento de Spark.
 
 ---
  
## Consultas mínimas desde AstraDB

Se armaron las siguientes Queries CQL para realizar uan evaluacion del estado de las tablas subidas a Cassandra yver que tan factible es realizar consultas de negocios reales.
Si bien se pedian 5 queries, se realizó una sexta respecto al rema sustentabilidad para asi mostrar mas  posibiliades de las tablas.

 **Queries**

```python
# (1) FinOps: Costos y requests diarios (rango de fechas)
    print("\n1. [FinOps] Costos y requests diarios (Últimos 7 días)")
    query_1 = f"""
        SELECT usage_date, service_name, daily_cost_usd, daily_requests
        FROM "{KEYSPACE}".org_daily_usage_by_service
        WHERE org_id = '{TARGET_ORG}'
        AND usage_date >= '{ref_date - timedelta(days=7)}'
    """
    rows = session.execute(query_1)
    print(f"{'Fecha':<12} | {'Servicio':<15} | {'Costo ($)':<10} | {'Requests'}")
    for r in rows:
        print(f"{str(r.usage_date):<12} | {r.service_name:<15} | {r.daily_cost_usd:<10.2f} | {int(r.daily_requests)}")


    # (2) FinOps: Top 3 servicios mas costosos (ultimos 14 dias)
    print(f"\n2. [FinOps] Top 3 Servicios más costosos (Desde {date_14_days_ago})")
    query_2 = f"""
        SELECT service_name, daily_cost_usd
        FROM "{KEYSPACE}".org_daily_usage_by_service
        WHERE org_id = '{TARGET_ORG}'
        AND usage_date >= '{date_14_days_ago}'
    """
    rows = session.execute(query_2)

    # agregacion client-side
    cost_map = {}
    for r in rows:
        cost_map[r.service_name] = cost_map.get(r.service_name, 0.0) + r.daily_cost_usd

    sorted_services = sorted(cost_map.items(), key=lambda x: x[1], reverse=True)[:3]

    print(f"{'Servicio':<15} | {'Costo Acum. ($)'}")
    for svc, cost in sorted_services:
        print(f"{svc:<15} | {cost:.2f}")


    # (3) Soporte: Tickets criticos y SLA breach rate (30 dias)
    print(f"\n3. [Soporte] Tickets críticos y Tasa SLA Breach (Desde {date_30_days_ago})")
    query_3 = f"""
        SELECT ticket_date, critical_tickets, sla_breach_rate
        FROM "{KEYSPACE}".org_daily_support_metrics
        WHERE org_id = '{TARGET_ORG}'
        AND ticket_date >= '{date_30_days_ago}'
    """
    rows = session.execute(query_3)

    # manejo por si no hay tickets
    if not rows:
        print("(Sin tickets en este periodo para esta org)")

    print(f"{'Fecha':<12} | {'Críticos':<8} | {'Tasa Breach %'}")
    for r in rows:
        rate_pct = r.sla_breach_rate * 100 if r.sla_breach_rate else 0.0
        print(f"{str(r.ticket_date):<12} | {r.critical_tickets:<8} | {rate_pct:.1f}%")


    # (4) Finanzas: Revenue mensual estimado (con tax)
    # nota: tomo desde usage
    print(f"\n4. [Finanzas] Revenue Mensual Estimado (Mes actual: {start_of_month})")

    query_4 = f"""
        SELECT daily_cost_usd
        FROM "{KEYSPACE}".org_daily_usage_by_service
        WHERE org_id = '{TARGET_ORG}'
        AND usage_date >= '{start_of_month}'
    """
    rows = session.execute(query_4)

    monthly_subtotal = sum([r.daily_cost_usd for r in rows])
    taxes = monthly_subtotal * 0.21 # 21% tax -tome el IVA como ejemplo, eso lo decide finanzas
    credits = 0.0
    total_revenue = monthly_subtotal + taxes - credits

    print(f"Subtotal Uso:    ${monthly_subtotal:.2f}")
    print(f"Impuestos (est): ${taxes:.2f}")
    print(f"TOTAL REVENUE:   ${total_revenue:.2f}")


    # (5) Producto: Consumo GenAI (tokens y costo)
    print("\n5. [Producto] Consumo GenAI (Tokens/Requests y Costo)")
    query_5 = f"""
        SELECT event_date, service_name, genai_requests_count, genai_daily_cost
        FROM "{KEYSPACE}".org_daily_genai_usage
        WHERE org_id = '{TARGET_ORG}'
        LIMIT 5
    """
    rows = list(session.execute(query_5))

    if not rows:
        print("(Esta organización no registra consumo de GenAI)")
    else:
        print(f"{'Fecha':<12} | {'Servicio':<12} | {'Tokens/Reqs':<12} | {'Costo ($)'}")
        for r in rows:
            print(f"{str(r.event_date):<12} | {r.service_name:<12} | {int(r.genai_requests_count):<12} | {r.genai_daily_cost:.2f}")

  # (6) Sustentabilidad
    print("\n6. [ESG] Huella de Carbono")
    query_4 = f"""
        SELECT usage_date, service_name, daily_carbon_kg
        FROM "{KEYSPACE}".org_daily_usage_by_service
        WHERE org_id = '{TARGET_ORG}'
        LIMIT 5
    """
    rows = session.execute(query_4)
    print(f"   {'Fecha':<12} | {'Servicio':<15} | {'Carbono (kgCO2)'}")
    print("   " + "-"*50)
    for r in rows:
        print(f"   {str(r.usage_date):<12} | {r.service_name:<15} | {f'{r.daily_carbon_kg:.4f}' if r.daily_carbon_kg is not None else 'None':<15}")
```
### Resultados de las queries - output de celda  `cassandra_queries.py`:

_Observación_ : la celda toma una organizacion random y genera las tablas.
```python console
Queries de negocios 

Organizacion: org_pja1wj0t
Fecha de Referencia: 2025-08-31
------------------------------------------------------------

1. [FinOps] Costos y requests diarios (Últimos 7 días)
Fecha        | Servicio        | Costo ($)  | Requests
2025-08-31   | compute         | 11.93      | 111
2025-08-31   | networking      | 2.51       | 234
2025-08-27   | compute         | 0.11       | 0

2. [FinOps] Top 3 Servicios más costosos (Desde 2025-08-17)
Servicio        | Costo Acum. ($)
compute         | 12.04
networking      | 2.51

3. [Soporte] Tickets críticos y Tasa SLA Breach (Desde 2025-08-01)
Fecha        | Críticos | Tasa Breach %
2025-08-23   | 0        | 100.0%
2025-08-22   | 0        | 0.0%
2025-08-20   | 0        | 0.0%

4. [Finanzas] Revenue Mensual Estimado (Mes actual: 2025-08-01)
Subtotal Uso:    $14.56
Impuestos (est): $3.06
TOTAL REVENUE:   $17.61

5. [Producto] Consumo GenAI (Tokens/Requests y Costo)
(Esta organización no registra consumo de GenAI)

6. [ESG] Huella de Carbono
   Fecha        | Servicio        | Carbono (kgCO2)
   --------------------------------------------------
   2025-08-31   | compute         | 0.0255         
   2025-08-31   | networking      | 0.0468         
   2025-08-27   | compute         | 0.0002         
   2025-08-05   | networking      | 0.0001         
   2025-07-24   | compute         | 0.0005

```


## Limitaciones principales:

-   El cálculo de Revenue se realiza en tiempo de consulta (On-the-fly) en lugar de pre-agregarse en un Mart físico, lo cual es aceptable para el volumen actual pero no escala bien
    
-   La detección de anomalías se reduce a un umbral simple sobre `cost_usd_increment` y a flags implícitos de calidad; no se utilizaron los tres métodos estadísticos sugeridos (z-score, MAD, percentiles) a los fines de simplificar la entrega.
    
-   La **gestión de `schema_version`** se limita a conservar la columna y explotar `carbon_kg` cuando existe; no hay lógica diferenciada v1/v2 ni backfill explícito para campos nuevos.
    
-  El pipeline tolera cambios de esquema (columna `schema_version`), pero no implementa lógica de backfilling automático ni tablas SCD Tipo 2 para cambios en dimensiones lentas (Organizaciones/Recursos).
    
-   La métricas de GenAI agregan `requests` como proxy de tokens; el agregado explícito de `genai_tokens` queda pendiente

-   Debido a las restricciones del entorno de Google Colab, la orquestación de tareas Batch y Streaming se simuló secuencialmente en un notebook. Un entorno productivo requeriría Cron/Apache Airflow para gestión de dependencias y reintentos.



## Trabajo futuro propuesto:

-   Implementar  un Hardening de Calidad en la capa Silver para incluir validaciones estadísticas (desviación estándar de costos) antes de la carga a Gold. en la capa Silver para incluir validaciones estadísticas (desviación estándar de costos) antes de la carga a Gold.

- Materializar la tabla de Revenue con contadores distribuidos para evitar el costo de agregación en lectura.

- Incorporar z-score, MAD y percentiles para flags de anomalía y consolidarlos en un mart consumible por FinOps.

- Implementar lógica específica por `schema_version` (v1/v2) y, si fuera necesario, tablas SCD para organizaciones/recursos.    

-   Migrar la ejecución a un orquestador (Airflow/cron + Prometheus/Grafana) y conectar directamente AstraDB a Power BI/Looker para dashboards productivos.

- Definir políticas de Backup y Disaster Recovery para el Data Lake y AstraDB. 

- Implementar cifrado en reposo/tránsito y controles de acceso (RBAC) para garantizar el cumplimiento de normativas (GDPR/SOC2) antes del despliegue productivo.
    if not rows:
        print("(Sin tickets en este periodo para esta org)")

    print(f"{'Fecha':<12} | {'Críticos':<8} | {'Tasa Breach %'}")
    for r in rows:
        rate_pct = r.sla_breach_rate * 100 if r.sla_breach_rate else 0.0
        print(f"{str(r.ticket_date):<12} | {r.critical_tickets:<8} | {rate_pct:.1f}%")


    # (4) Finanzas: Revenue mensual estimado (con tax)
    # nota: tomo desde usage
    print(f"\n4. [Finanzas] Revenue Mensual Estimado (Mes actual: {start_of_month})")

    query_4 = f"""
        SELECT daily_cost_usd
        FROM "{KEYSPACE}".org_daily_usage_by_service
        WHERE org_id = '{TARGET_ORG}'
        AND usage_date >= '{start_of_month}'
    """
    rows = session.execute(query_4)

    monthly_subtotal = sum([r.daily_cost_usd for r in rows])
    taxes = monthly_subtotal * 0.21 # 21% tax -tome el IVA como ejemplo, eso lo decide finanzas
    credits = 0.0
    total_revenue = monthly_subtotal + taxes - credits

    print(f"Subtotal Uso:    ${monthly_subtotal:.2f}")
    print(f"Impuestos (est): ${taxes:.2f}")
    print(f"TOTAL REVENUE:   ${total_revenue:.2f}")


    # (5) Producto: Consumo GenAI (tokens y costo)
    print("\n5. [Producto] Consumo GenAI (Tokens/Requests y Costo)")
    query_5 = f"""
        SELECT event_date, service_name, genai_requests_count, genai_daily_cost
        FROM "{KEYSPACE}".org_daily_genai_usage
        WHERE org_id = '{TARGET_ORG}'
        LIMIT 5
    """
    rows = list(session.execute(query_5))

    if not rows:
        print("(Esta organización no registra consumo de GenAI)")
    else:
        print(f"{'Fecha':<12} | {'Servicio':<12} | {'Tokens/Reqs':<12} | {'Costo ($)'}")
        for r in rows:
            print(f"{str(r.event_date):<12} | {r.service_name:<12} | {int(r.genai_requests_count):<12} | {r.genai_daily_cost:.2f}")

  # (6) Sustentabilidad
    print("\n6. [ESG] Huella de Carbono")
    query_4 = f"""
        SELECT usage_date, service_name, daily_carbon_kg
        FROM "{KEYSPACE}".org_daily_usage_by_service
        WHERE org_id = '{TARGET_ORG}'
        LIMIT 5
    """
    rows = session.execute(query_4)
    print(f"   {'Fecha':<12} | {'Servicio':<15} | {'Carbono (kgCO2)'}")
    print("   " + "-"*50)
    for r in rows:
        print(f"   {str(r.usage_date):<12} | {r.service_name:<15} | {f'{r.daily_carbon_kg:.4f}' if r.daily_carbon_kg is not None else 'None':<15}")
```
### Resultados de las queries - output de celda  `cassandra_queries.py`:

_Observación_ : la celda toma una organizacion random y genera las tablas.
```python console
Queries de negocios 

Organizacion: org_pja1wj0t
Fecha de Referencia: 2025-08-31
------------------------------------------------------------

1. [FinOps] Costos y requests diarios (Últimos 7 días)
Fecha        | Servicio        | Costo ($)  | Requests
2025-08-31   | compute         | 11.93      | 111
2025-08-31   | networking      | 2.51       | 234
2025-08-27   | compute         | 0.11       | 0

2. [FinOps] Top 3 Servicios más costosos (Desde 2025-08-17)
Servicio        | Costo Acum. ($)
compute         | 12.04
networking      | 2.51

3. [Soporte] Tickets críticos y Tasa SLA Breach (Desde 2025-08-01)
Fecha        | Críticos | Tasa Breach %
2025-08-23   | 0        | 100.0%
2025-08-22   | 0        | 0.0%
2025-08-20   | 0        | 0.0%

4. [Finanzas] Revenue Mensual Estimado (Mes actual: 2025-08-01)
Subtotal Uso:    $14.56
Impuestos (est): $3.06
TOTAL REVENUE:   $17.61

5. [Producto] Consumo GenAI (Tokens/Requests y Costo)
(Esta organización no registra consumo de GenAI)

6. [ESG] Huella de Carbono
   Fecha        | Servicio        | Carbono (kgCO2)
   --------------------------------------------------
   2025-08-31   | compute         | 0.0255         
   2025-08-31   | networking      | 0.0468         
   2025-08-27   | compute         | 0.0002         
   2025-08-05   | networking      | 0.0001         
   2025-07-24   | compute         | 0.0005

```


## Limitaciones principales:

-   El cálculo de Revenue se realiza en tiempo de consulta (On-the-fly) en lugar de pre-agregarse en un Mart físico, lo cual es aceptable para el volumen actual pero no escala bien
    
-   La detección de anomalías se reduce a un umbral simple sobre `cost_usd_increment` y a flags implícitos de calidad; no se utilizaron los tres métodos estadísticos sugeridos (z-score, MAD, percentiles) a los fines de simplificar la entrega.
    
-   La **gestión de `schema_version`** se limita a conservar la columna y explotar `carbon_kg` cuando existe; no hay lógica diferenciada v1/v2 ni backfill explícito para campos nuevos.
    
-  El pipeline tolera cambios de esquema (columna `schema_version`), pero no implementa lógica de backfilling automático ni tablas SCD Tipo 2 para cambios en dimensiones lentas (Organizaciones/Recursos).
    
-   La métricas de GenAI agregan `requests` como proxy de tokens; el agregado explícito de `genai_tokens` queda pendiente

-   Debido a las restricciones del entorno de Google Colab, la orquestación de tareas Batch y Streaming se simuló secuencialmente en un notebook. Un entorno productivo requeriría Cron/Apache Airflow para gestión de dependencias y reintentos.



## Trabajo futuro propuesto:

-   Implementar  un Hardening de Calidad en la capa Silver para incluir validaciones estadísticas (desviación estándar de costos) antes de la carga a Gold. en la capa Silver para incluir validaciones estadísticas (desviación estándar de costos) antes de la carga a Gold.

- Materializar la tabla de Revenue con contadores distribuidos para evitar el costo de agregación en lectura.

- Incorporar z-score, MAD y percentiles para flags de anomalía y consolidarlos en un mart consumible por FinOps.

- Implementar lógica específica por `schema_version` (v1/v2) y, si fuera necesario, tablas SCD para organizaciones/recursos.    

-   Migrar la ejecución a un orquestador (Airflow/cron + Prometheus/Grafana) y conectar directamente AstraDB a Power BI/Looker para dashboards productivos.

- Definir políticas de Backup y Disaster Recovery para el Data Lake y AstraDB. 

- Implementar cifrado en reposo/tránsito y controles de acceso (RBAC) para garantizar el cumplimiento de normativas (GDPR/SOC2) antes del despliegue productivo.