# eCommerce Cart Abandonment: Medallion Architecture & Feature Store

Este repositorio contiene la arquitectura completa de datos para un sistema de predicción de abandono de carrito de compra. El proyecto implementa un pipeline robusto de ingeniería de datos utilizando **Delta Live Tables (DLT)**, **Unity Catalog** y **Databricks Feature Store** para procesar un volumen de 17 millones de registros.

## 🏗️ Estructura del Proyecto

Basado en la organización de carpetas del repositorio:

```text
medallion_pipeline/
├── resources/                # Definiciones YAML de Pipelines y Jobs (Lakeflow)
├── src/medallion_pipeline_etl/
│   ├── notebooks/            # Orquestación y registro en Online Feature Store
│   ├── rules/                # Reglas de calidad declarativas (Expectations)
│   └── transformations/      # Lógica del pipeline Medallón (Capas 01, 02, 03)
├── databricks.yml            # Configuración de Databricks Asset Bundle
└── pyproject.toml            # Dependencias y configuración de Python

🚀 Arquitectura de Datos (Medallón)
El pipeline está diseñado en tres capas lógicas dentro de Databricks:

1. Capa Bronce (01_bronze_ingestion.py)
Ingesta Incremental: Carga de eventos de navegación, etiquetas de abandono y perfiles de usuario desde la landing_zone.

Almacenamiento: Tablas Delta optimizadas con metadatos de origen.

2. Capa Plata (02_silver_transformation.py)
Calidad de Datos: Aplicación de reglas definidas en src/.../rules/ para filtrar registros anómalos hacia tablas de Cuarentena (DLQ).

Gestión de Históricos: Implementación de SCD Tipo 2 (AUTO CDC) en la tabla de usuarios para garantizar la trazabilidad temporal.

Integración: Stream-stream joins con configuración de Watermarks de 30 días para enriquecer eventos de carrito con sus etiquetas correspondientes.

3. Capa Oro (03_gold_...)
Especializada en la generación de características para Machine Learning:

Behavioral Features: Agregaciones dinámicas en ventanas rodantes (1h, 24h, 7d, 30d) para medir la intensidad del usuario.

User Profile: Derivación de atributos demográficos estáticos y segmentación de lealtad.

Spine Table: Construcción del dataset ancla para entrenamiento con la variable objetivo will_abandon.

🛠️ Feature Store y Operaciones (MLOps)
Registro de Características: Uso de la API de Databricks Feature Engineering para el registro automático de tablas Oro mediante claves primarias y series temporales.

Online Serving: Sincronización incremental (vía Change Data Feed) hacia Lakebase para permitir inferencias con latencia <10ms.

Orquestación: Job programado con ejecución horaria, alertas de fallo automáticas y monitoreo de umbrales de duración (Warning: 15m / Timeout: 30m).
