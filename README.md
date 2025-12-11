# Smart Device Analytics – Microsoft Fabric (Lakehouse + Warehouse + PySpark)
> Proyecto de ingeniería de datos con ingesta incremental y full load en Lakehouse, transformación con PySpark + Dataflows Gen2, y análisis en SQL Warehouse con procedimientos almacenados, todo orquestado con pipelines Fabric.

[🎥 Demo: https://youtu.be/TU-DEMO] (reemplaza con tu enlace real)

---

## 1) Elevator pitch
Este proyecto implementa un flujo de datos robusto y automatizado en Microsoft Fabric:
- Ingesta de archivos CSV desde Azure Data Lake Storage (shortcuts).
- Transformación en capas Bronze → Silver → Gold con PySpark y Dataflows Gen2.
- Función merge_delta_lake para cargas incrementales eficientes.
- Procedimientos almacenados en SQL Warehouse para cargas completas e incrementales.
- Pipelines Fabric con control de errores y ejecución secuencial.

## 2) Arquitectura
Azure Data Lake Storage ──► Lakehouse (Bronze, Silver, Gold) ──► SQL Warehouse ──► Pipelines Fabric ▲ └────────── ingesta, transformación, cargas full/incrementales, control de errores ───────────┘

- **Fuente**: Archivos CSV en Azure Data Lake Storage.

- **Ingesta**: Notebooks PySpark con esquema explícito y validaciones.

- **Transformación**:

Bronze → Silver: limpieza y normalización en formato Delta.

Silver → Gold: cargas incrementales con merge_delta_lake y cargas completas con Dataflows Gen2.

Warehouse: Procedimientos almacenados (sp_full_load, sp_incremental_load) para manejar cargas según tipo.

Orquestación: Pipelines Fabric que ejecutan ingestión, transformación, análisis y refresco, con lógica de control (If Folder Exists, email error + fail).

## 3) Procesos de Ingeniería de Datos
3.1 Ingesta:
Lectura de CSV desde Bronze con PySpark.

Definición manual de esquemas para optimizar rendimiento.

Validación de existencia de carpeta antes de ingesta.

Pipeline con condición: si no existe carpeta → email error + fail.

3.2 Transformación
Conversión a Delta Lake en Silver.

Limpieza de columnas, renombrado y normalización.

Función merge_delta_lake para cargas incrementales: inserta/actualiza registros si la tabla existe, crea tabla si no.

Dataflows Gen2 para cargas completas (dimensiones estáticas).

3.3 Análisis en Warehouse
Tablas Gold cargadas al SQL Warehouse.

Procedimientos almacenados:

sp_full_load: recrea tabla completa desde Gold.

sp_incremental_load: inserta/actualiza registros nuevos.

Invocación de procedimientos vía pipelines con parámetros dinámicos.

3.4 Orquestación
Pipelines Fabric:

Ingesta: Bronze → Silver.

Transformación: Silver → Gold.

Análisis: Gold → Warehouse con stored procedures.

Principal: ejecuta todos en secuencia, con control de errores.

4) KPIs de Ingeniería de Datos
% de cargas incrementales exitosas.

Tiempo promedio de ingesta por lote.

Registros insertados vs actualizados en cada merge.

Diferencia entre cargas full vs incrementales.

Número de procedimientos almacenados ejecutados por ciclo.

5) Stack usado en el proyecto
Microsoft Fabric: Lakehouse, Warehouse, Pipelines, Dataflows Gen2.

PySpark: notebooks para ingesta y transformación, función merge_delta_lake.

SQL Warehouse: procedimientos almacenados para cargas completas e incrementales.

Azure Data Lake Storage: fuente de datos crudos con shortcuts.

GitHub: código, documentación, versionado.
