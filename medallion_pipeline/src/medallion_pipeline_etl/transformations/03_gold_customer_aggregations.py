"""
Propósito: Construir la tabla de características de comportamiento (Behavioral Features)
para el modelo de Abandono de Carrito. Calcula agregaciones temporales por usuario 
(valor de carrito, categorías visitadas, frecuencia de sesiones).

Esta tabla usa ventanas rodantes para garantizar el 'point-in-time correctness', 
asegurando que el modelo no 'vea el futuro' durante el entrenamiento.
"""

###############################################################################
# Imports
###############################################################################

import pyspark.pipelines as dp
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    count,
    collect_set,
    lit,
    max,
    min,
    size,
    sum,
    when
)

###############################################################################
# Configuración
###############################################################################

silver_events_source = "silver_abandonment_events"
EPSILON = 1e-6  # Evita división por cero

###############################################################################
# Tabla Final: Agregaciones de comportamiento eCommerce
###############################################################################

gold_aggregations_table_name = "gold_user_behavior_features"
gold_aggregations_comment = """
Tabla de características para ML. Consolida el comportamiento del usuario en 4 
ventanas temporales (1h, 24h, 7d, 30d). Incluye métricas de valor de carrito, 
diversidad de categorías y frecuencia de abandono previo.
"""

# Esquema adaptado a vuestros campos de eCommerce
gold_aggregations_schema = """
    user_id STRING NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    count_events_1h BIGINT,
    sum_cart_value_1h DOUBLE,
    avg_cart_value_1h DOUBLE,
    distinct_categories_1h INT,
    count_sessions_24h BIGINT,
    sum_cart_value_24h DOUBLE,
    avg_cart_value_24h DOUBLE,
    max_cart_value_24h DOUBLE,
    distinct_categories_24h INT,
    count_add_to_cart_24h BIGINT,
    count_events_7d BIGINT,
    sum_cart_value_7d DOUBLE,
    distinct_categories_7d INT,
    count_events_30d BIGINT,
    sum_cart_value_30d DOUBLE,
    avg_cart_value_30d DOUBLE,
    num_abandoned_confirmed_30d BIGINT,
    cart_value_24h_vs_avg_30d_ratio DOUBLE,
    CONSTRAINT gold_user_features_pk PRIMARY KEY (user_id, event_timestamp TIMESERIES)
"""

@dp.table(
    name = gold_aggregations_table_name,
    comment = gold_aggregations_comment,
    schema = gold_aggregations_schema,
    table_properties = {"delta.enableChangeDataFeed": "true"}
)
def gold_user_behavior_features():
    """
    Calcula agregaciones por usuario usando ventanas rodantes (Rolling Windows)
    para evitar el 'data leakage'.
    """
    df_silver = spark.read.table(silver_events_source)

    # Convertimos a milisegundos para precisión en el orden de eventos
    df = df_silver.withColumn("ts_ms", (col("event_timestamp").cast("double") * 1000).cast("long"))

    # Definición de ventanas (excluyendo el evento actual con -1 para evitar sesgos)
    # 1h, 24h, 7d, 30d
    w_1h  = Window.partitionBy("user_id").orderBy("ts_ms").rangeBetween(-3_600_000, -1)
    w_24h = Window.partitionBy("user_id").orderBy("ts_ms").rangeBetween(-86_400_000, -1)
    w_7d  = Window.partitionBy("user_id").orderBy("ts_ms").rangeBetween(-7 * 86_400_000, -1)
    w_30d = Window.partitionBy("user_id").orderBy("ts_ms").rangeBetween(-30 * 86_400_000, -1)

    df_agg = df.select(
        col("user_id"),
        col("event_timestamp"),

        # Agregaciones 1 hora: Intensidad inmediata
        count("session_id").over(w_1h).alias("count_events_1h"),
        coalesce(sum("cart_value").over(w_1h), lit(0.0)).alias("sum_cart_value_1h"),
        avg("cart_value").over(w_1h).alias("avg_cart_value_1h"),
        size(collect_set("item_category").over(w_1h)).alias("distinct_categories_1h"),

        # Agregaciones 24 horas: Interés diario
        count("session_id").over(w_24h).alias("count_sessions_24h"),
        coalesce(sum("cart_value").over(w_24h), lit(0.0)).alias("sum_cart_value_24h"),
        avg("cart_value").over(w_24h).alias("avg_cart_value_24h"),
        max("cart_value").over(w_24h).alias("max_cart_value_24h"),
        size(collect_set("item_category").over(w_24h)).alias("distinct_categories_24h"),
        coalesce(
            sum(when(col("event_type") == 'add_to_cart', 1).otherwise(0)).over(w_24h), lit(0)
        ).alias("count_add_to_cart_24h"),

        # Agregaciones 7 días: Tendencia semanal
        count("session_id").over(w_7d).alias("count_events_7d"),
        coalesce(sum("cart_value").over(w_7d), lit(0.0)).alias("sum_cart_value_7d"),
        size(collect_set("item_category").over(w_7d)).alias("distinct_categories_7d"),

        # Agregaciones 30 días: Historial de comportamiento
        count("session_id").over(w_30d).alias("count_events_30d"),
        coalesce(sum("cart_value").over(w_30d), lit(0.0)).alias("sum_cart_value_30d"),
        avg("cart_value").over(w_30d).alias("avg_cart_value_30d"),
        # Variable crítica: ¿Cuántas veces ha abandonado ya en el último mes?
        coalesce(
            sum(when(col("will_abandon") == 1, 1).otherwise(0)).over(w_30d), lit(0)
        ).alias("num_abandoned_confirmed_30d"),
    )

    # Ratio de gasto: Valor actual vs media histórica
    df_final = df_agg.withColumn(
        "cart_value_24h_vs_avg_30d_ratio",
        coalesce(
            col("sum_cart_value_24h") / (col("avg_cart_value_30d") + lit(EPSILON)),
            lit(1.0)
        )
    )

    return df_final