"""
Propósito: Construir la tabla de características de perfil de usuario para el Feature Store.
Toma el historial SCD2 de la capa Silver y deriva atributos demográficos estáticos.
"""

###############################################################################
# Imports
###############################################################################

import pyspark.pipelines as dp
from pyspark.sql.functions import col, when

###############################################################################
# Configuración y Constantes
###############################################################################

# REQUISITO 3.6.2: Habilitar Change Data Feed para sincronización eficiente con el Online Store
gold_profile_properties = {"delta.enableChangeDataFeed": "true"}

# Origen de datos (vuestra tabla SCD2 de Silver)
silver_users_source = "silver_users_history"

gold_profile_table_name = "gold_user_profile"
gold_profile_comment = """
Tabla de características estáticas (User Profile). Contiene el historial SCD2 
y atributos derivados como grupos de edad y segmentación por tasa de retorno.
"""

# REQUISITO 3.6.2: Definición de esquema con CONSTRAINT de PRIMARY KEY y TIMESERIES
# Esto permite el registro automático en el Feature Store de Unity Catalog.
gold_profile_schema = """
    user_id STRING NOT NULL,
    user_type STRING,
    age INT,
    gender STRING,
    country STRING,
    preferred_device STRING,
    registration_date DATE,
    favourite_category STRING,
    return_rate DOUBLE,
    has_app_installed INT,
    email_opt_in INT,
    push_opt_in INT,
    __START_AT TIMESTAMP NOT NULL,
    __END_AT TIMESTAMP,
    age_group STRING NOT NULL,
    loyalty_segment STRING NOT NULL,
    CONSTRAINT gold_user_profile_pk PRIMARY KEY (user_id, __START_AT TIMESERIES)
"""

###############################################################################
# Feature Engineering (Perfiles Estáticos)
###############################################################################

# REQUISITO 3.6.2: Uso de @dp.materialized_view + spark.read (Batch)
# Esto garantiza que se capturen las actualizaciones de __END_AT generadas por el SCD2.
@dp.materialized_view(
    name = gold_profile_table_name,
    comment = gold_profile_comment,
    table_properties = gold_profile_properties,
    schema = gold_profile_schema
)
def gold_user_profile():
    """
    Lee el historial completo de usuarios y deriva características demográficas.
    Mantiene la validez temporal (__START_AT, __END_AT) para el Point-in-Time Join.
    """
    df_users = spark.read.table(silver_users_source)

    df_final = df_users.select(
        col("user_id"),
        col("user_type"),
        col("age"),
        col("gender"),
        col("country"),
        col("preferred_device"),
        col("registration_date"),
        col("favourite_category"),
        col("return_rate"),
        col("has_app_installed"),
        col("email_opt_in"),
        col("push_opt_in"),
        
        # Columnas técnicas de SCD2 fundamentales para el Feature Store
        col("__START_AT"),
        col("__END_AT"),

        # Características derivadas (Feature Engineering)
        when(col("age") < 25, "Gen Z")
        .when((col("age") >= 25) & (col("age") < 45), "Millennial")
        .otherwise("Senior")
        .alias("age_group"),

        when(col("return_rate") > 0.3, "High Returner")
        .when(col("user_type") == "loyalty", "Loyal VIP")
        .otherwise("Standard")
        .alias("loyalty_segment")
    )

    return df_final