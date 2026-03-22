"""
Propósito: Construir la tabla 'Spine' (Columna Vertebral) de la capa Oro.
Esta tabla es el punto de partida para el entrenamiento del modelo de ML.
Contiene los IDs, el timestamp, la variable objetivo (will_abandon) y 
las características disponibles en tiempo real.
"""

###############################################################################
# Imports
###############################################################################

import pyspark.pipelines as dp
from pyspark.sql.functions import col, date_format, to_timestamp

###############################################################################
# Ingeniería de Características (Creación de la Spine)
###############################################################################

gold_spine_table_name = "gold_abandonment_spine"
gold_spine_comment = """
Tabla ancla para el modelo de ML. Contiene las claves primarias (user_id, session_id), 
el momento del evento, la etiqueta de abandono y características de la sesión 
disponibles en tiempo real.
"""

silver_events_source = "silver_abandonment_events"

@dp.table(name = gold_spine_table_name, comment = gold_spine_comment)
def gold_abandonment_spine():
    """
    Lee los eventos enriquecidos de Silver para construir la base del dataset.
    Mantiene únicamente lo necesario para que el Feature Store realice 
    los cruces PiT (Point-in-Time) después.
    """
    df_events = spark.readStream.table(silver_events_source)

    # REQUISITO TÉCNICO: Bypass del optimizador para eliminar metadatos de watermark.
    # Se formatea la fecha a string y se reconvierte a timestamp para asegurar 
    # que "label_available_date" esté limpia de estados de streaming.
    df_events = df_events.withColumn(
        "label_available_date",
        to_timestamp(date_format(col("label_available_date"), "yyyy-MM-dd HH:mm:ss.SSS"))
    )

    # Selección de campos core (la "columna vertebral")
    df_spine = df_events.select(
        # Identificadores primarios y tiempo del evento
        col("session_id"),
        col("user_id"),
        col("event_timestamp"),

        # Variable objetivo (Label)
        col("will_abandon"),
        col("label_available_date"),

        # Características en TIEMPO REAL (disponibles en el payload de la web)
        col("item_category"),
        col("cart_value"),
        col("event_type")
        # Nota: No unimos aquí perfiles ni agregaciones; el Feature Store 
        # lo hará automáticamente en la fase de modelado.
    )

    return df_spine