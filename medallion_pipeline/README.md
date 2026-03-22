# Abandono de carrito - Pipeline

Este repositorio contiene la solución integral de ingeniería de datos para la predicción de abandono de carrito de compra. Implementa una **Arquitectura Medallion** robusta sobre **Databricks**, integrando **Delta Live Tables**, **Unity Catalog** y el **Feature Store** para habilitar modelos de Machine Learning en tiempo real.

## 📂 Estructura del Repositorio

A continuación se detalla la función de los componentes principales en la raíz del proyecto:

* **[.vscode/](./.vscode/)**: Configuraciones del entorno de desarrollo para asegurar la consistencia del código entre colaboradores.
* **[resources/](./resources/)**: Contiene los manifiestos YAML que definen la infraestructura como código, incluyendo la configuración del Pipeline DLT y la orquestación del Job en Lakeflow.
* **[src/medallion_pipeline_etl/](./src/medallion_pipeline_etl/)**: Directorio principal que alberga la lógica de negocio, reglas de calidad y transformaciones de las capas Bronce, Plata y Oro.
* **`databricks.yml`**: Archivo de configuración para **Databricks Asset Bundles (DABs)**, permitiendo despliegues automatizados y consistentes entre entornos.
* **`pyproject.toml`**: Define las dependencias de Python y los requisitos del sistema necesarios para el procesamiento de los 17 millones de registros del dataset.

## 🏗️ Flujo de Trabajo

El sistema está orquestado mediante un **Databricks Job** que encadena dos fases críticas:

1. **Pipeline de Datos (DLT)**: Procesa el flujo desde la ingesta (`01_bronze`) hasta la generación de características (`03_gold`), aplicando gobernanza de datos mediante una cola de cuarentena (DLQ) y gestión de históricos SCD Tipo 2.
2. **Registro de Features**: Un proceso posterior sincroniza de forma incremental las tablas de la capa Oro con el **Online Feature Store** (Lakebase) para permitir inferencias con latencia inferior a 10ms.

## 📊 Monitoreo y Alertas

El pipeline cuenta con monitoreo preventivo configurado:
* **Alertas**: Notificaciones por correo electrónico automáticas ante fallos de ejecución para los administradores del sistema (Eugenio y Norberto).
* **Umbrales**: Advertencias de duración a los 15 minutos y tiempo de espera crítico a los 30 minutos para garantizar la eficiencia operativa.
