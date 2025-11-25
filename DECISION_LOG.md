# Log de Decisiones

## 1.Arquitectura: Lambda
**Decisión:** Se implemento una Arquitectura Lambda (Batch Layer + Speed Layer).
**Justificación:**
*   **Batch Layer:** Necesaria para procesar los datos maestros enCSV que llegan con frecuencia diaria/mensual y para recalcular la historia.
*   **Speed Layer:** Requerida para dar visibilidad inmediata de los costos (`usage_events`)

## 2. Formato de Almacenamiento: Parquet
**Decisión:** Uso de Apache Parquet
**Justificación:**
*   Formato columnar ideal para consultas analíticas (OLAP).
*   Soporta evolución de esquemas y mantiene los tipos de datos (a diferencia de CSV).

## 3. Estrategia de Particionamiento
**Decisión:** Particionar tablas transaccionales por `event_date` o `month`.
**Justificación:**
*   Los patrones de consulta y carga son temporales.
*   Permite optimizar las cargas incrementales (reemplaza solo la particion del dia procesado).

## 4. Modelado en Cassandra (Serving)
**Decisión:** Tabla `org_daily_usage_by_service` con PK `((org_id), usage_date, service_name)`.
**Justificación:**
*   **Partition Key (`org_id`):** El caso de uso principal es "Mostrar costos para el Cliente X". Esto asegura que todos los datos de un cliente vivan en el mismo nodo.
*   **Clustering Columns (`usage_date`, `service_name`):** Permite ordenar el historial cronológicamente y filtrar por servicio dentro de una organización sin scanear toda la partición.

## 5. Estrategia de Calidad y Cuarentena
**Decisión:** Uso de "Left Anti Join" para la escritura de errores.
**Justificación:**
*   En vez de hacer un append de errores, verifico si el ID del evento fallido ya existe en la carpeta de cuarentena.
*   Garantiza que no se encucien los logs de errores con duplicados si reiniciamos el pipeline varias veces.