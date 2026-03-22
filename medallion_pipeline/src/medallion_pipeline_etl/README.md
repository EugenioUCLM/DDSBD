# ETL Pipeline

Este directorio contiene el código fuente principal del pipeline de datos, organizado según la arquitectura de Medallion y diseñado para ejecutarse sobre **Delta Live Tables (DLT)**.

## 📂 Estructura del Directorio

### 1. [rules/](./rules/)
Contiene la lógica declarativa de las **Expectations** (reglas de calidad).
* `customers.py`: Validaciones para perfiles de usuario (edad, tipos de usuario).
* `labels.py`: Reglas para la integridad de la variable objetivo.
* `transactions.py`: Validaciones financieras para los eventos de carrito.

### 2. [transformations/](./transformations/)
Implementación de las tres capas de procesamiento de datos:
* **`01_bronze_ingestion.py`**: Ingesta incremental desde la `landing_zone` hacia Delta.
* **`02_silver_transformation.py`**: Limpieza de datos (DLQ), gestión de históricos **SCD Tipo 2** y unión de flujos con **Watermarks**.
* **`03_gold_customer_aggregations.py`**: Generación de características de comportamiento mediante ventanas rodantes (1h, 24h, 7d, 30d).
* **`03_gold_customer_profile.py`**: Vista materializada de perfiles estáticos optimizada para el Feature Store.
* **`03_gold_abandonment_spine.py`**: Tabla ancla (Spine) con etiquetas y características en tiempo real para entrenamiento.

### 3. [notebooks/](./notebooks/)
Tareas de orquestación post-pipeline:
* **`04_Feature_Store_Registration.py`**: Sincronización incremental de la capa Oro con el **Online Feature Store** (Lakebase) para inferencia de baja latencia.

## 🛠️ Tecnologías Utilizadas
* **Delta Live Tables (DLT)**: Orquestación de dependencias y flujo de datos.
* **Unity Catalog**: Gobernanza y registro de Feature Tables.
* **Change Data Feed (CDF)**: Propagación eficiente de cambios hacia el almacén online.

## 📋 Requisitos de Ejecución
Este código debe configurarse en un **Databricks Job** que encadene el pipeline DLT de transformaciones con el notebook de registro en el Feature Store, asegurando la coherencia temporal (*Point-in-Time*) de los datos.
