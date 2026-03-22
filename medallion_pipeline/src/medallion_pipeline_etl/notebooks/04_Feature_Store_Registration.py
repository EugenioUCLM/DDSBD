# Databricks notebook source
# MAGIC %md
# MAGIC # Publicación en el `Online Feature Store` - Abandono de Carrito
# MAGIC Este script sincroniza las características de la capa Gold con el almacenamiento de baja latencia (Lakebase).

# COMMAND ----------

# MAGIC %pip install databricks-feature-engineering>=0.13.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient

# CONFIGURACIÓN DEL PROYECTO
catalog = "workspace"
database = "abandono_carrito_comercio_electronico" # Vuestro esquema real

# Tablas de origen (Offline - Delta)
gold_user_profile_table = f"{catalog}.{database}.gold_user_profile"
gold_user_behavior_table = f"{catalog}.{database}.gold_user_behavior_features"

# Nombre del almacén online y tablas de destino (Online - Lakebase)
online_store_name = "abandono_carrito_online_store"
online_profile_table = f"{catalog}.{database}.online_user_profile"
online_behavior_table = f"{catalog}.{database}.online_user_behavior"

# Instanciar el cliente
fe = FeatureEngineeringClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sincronización con el Online Store
# MAGIC Sincronizamos de forma incremental (TRIGGERED) aprovechando el Change Data Feed.

# COMMAND ----------

publish_mode = "TRIGGERED" 

# 1. Publicar el Perfil de Usuario (Estático)
# fe.publish_table(
#     online_store = online_store_name,
#     source_table_name = gold_user_profile_table,
#     online_table_name = online_profile_table,
#     publish_mode = publish_mode
# )

# 2. Publicar el Comportamiento del Usuario (Agregaciones)
# fe.publish_table(
#     online_store = online_store_name,
#     source_table_name = gold_user_behavior_table,
#     online_table_name = online_behavior_table,
#     publish_mode = publish_mode
# )