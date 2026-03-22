"""
Capa Silver - Proyecto Abandono de Carrito.
Implementación profesional de arquitectura de Cuarentena (DLQ), 
AUTO CDC (SCD Tipo 2) y Stream-Stream Join con Watermarks.
"""

import pyspark.pipelines as dp
from pyspark.sql.functions import coalesce, col, expr, to_timestamp
from rules import get_rules # Importación de vuestro motor modular de reglas

###############################################################################
# 1. DIMENSIÓN USUARIOS (Cuarentena + SCD Tipo 2)
###############################################################################

# REQUISITO 3.5.2: Crear tabla física de cuarentena (DLQ) de forma anticipada
cust_quarantine_table = "silver_quarantine_customers"
dp.create_streaming_table(
    name = cust_quarantine_table,
    comment = "DLQ: Registros de usuarios que fallaron las reglas de calidad."
)

cust_rules_dict = get_rules("customers")
cust_quarantine_expr = "NOT (" + " AND ".join(cust_rules_dict.values()) + ")"

# REQUISITO 3.5.2: Tabla temporal (Hub de enrutamiento) con flag is_quarantined
@dp.table(name = "tmp_eval_customers", temporary = True)
@dp.expect_all(cust_rules_dict)
def eval_customers():
    return (
        spark.readStream
             .option("skipChangeCommits", "true") # Requisito para evitar paros por sobreescritura
             .table("bronze_customers")
             # REQUISITO 3.5.3: Imputación para asegurar clave de secuencia en SCD2
             .withColumn("user_updated_at", coalesce(col("user_updated_at"), col("registration_date")))
             .withColumn("is_quarantined", expr(cust_quarantine_expr))
    )

# REQUISITO 3.5.2: Bifurcación mediante append_flow y view
@dp.append_flow(target = cust_quarantine_table, name = "flow_quarantine_cust")
def quarantine_customers():
    """Envía registros inválidos a la tabla física descartando el flag."""
    return spark.readStream.table("tmp_eval_customers").filter("is_quarantined = true").drop("is_quarantined")

@dp.view(name = "vw_clean_customers")
def clean_customers():
    """Expone registros válidos sin materializar datos adicionales en disco."""
    return spark.readStream.table("tmp_eval_customers").filter("is_quarantined = false").drop("is_quarantined")

# REQUISITO 3.5.3: Gestión de históricos mediante AUTO CDC (SCD Tipo 2)
dp.create_streaming_table(
    name = "silver_users_history", 
    comment = "Dimensión histórica de usuarios (Point-in-time correctness)."
)

dp.create_auto_cdc_flow(
    target = "silver_users_history",
    source = "vw_clean_customers",
    keys = ["user_id"],
    sequence_by = col("user_updated_at"),
    # EXCLUSIÓN CRÍTICA: Evita ruido espurio en el histórico
    except_column_list = ["ingestion_timestamp", "source_file"],
    stored_as_scd_type = "2" # Genera automáticamente __START_AT y __END_AT
)

###############################################################################
# 2. EVENTOS DE CARRITO (Cuarentena + Tipado)
###############################################################################

tx_quarantine_table = "silver_quarantine_cart_events"
dp.create_streaming_table(name = tx_quarantine_table)

tx_rules = get_rules("transactions")
tx_quarantine_expr = "NOT (" + " AND ".join(tx_rules.values()) + ")"

@dp.table(name = "tmp_eval_cart_events", temporary = True)
@dp.expect_all(tx_rules)
def eval_cart_events():
    return (
        spark.readStream
             .option("skipChangeCommits", "true")
             .table("bronze_transactions")
             .withColumn("event_timestamp", to_timestamp(col("event_timestamp")))
             .withColumn("is_quarantined", expr(tx_quarantine_expr))
    )

@dp.append_flow(target = tx_quarantine_table)
def quarantine_cart_events():
    return spark.readStream.table("tmp_eval_cart_events").filter("is_quarantined = true").drop("is_quarantined")

@dp.view(name = "vw_clean_cart_events")
def clean_cart_events():
    return spark.readStream.table("tmp_eval_cart_events").filter("is_quarantined = false").drop("is_quarantined")

###############################################################################
# 3. ETIQUETAS DE ABANDONO (Cuarentena + Tipado)
###############################################################################

lbl_quarantine_table = "silver_quarantine_labels"
dp.create_streaming_table(name = lbl_quarantine_table)

lbl_rules = get_rules("labels")
lbl_quarantine_expr = "NOT (" + " AND ".join(lbl_rules.values()) + ")"

@dp.table(name = "tmp_eval_labels", temporary = True)
@dp.expect_all(lbl_rules)
def eval_labels():
    return (
        spark.readStream
             .option("skipChangeCommits", "true")
             .table("bronze_labels")
             .withColumn("label_available_date", to_timestamp(col("label_available_date")))
             .withColumn("is_quarantined", expr(lbl_quarantine_expr))
    )

@dp.append_flow(target = lbl_quarantine_table)
def quarantine_labels():
    return spark.readStream.table("tmp_eval_labels").filter("is_quarantined = true").drop("is_quarantined")

@dp.view(name = "vw_clean_labels")
def clean_labels():
    return spark.readStream.table("tmp_eval_labels").filter("is_quarantined = false").drop("is_quarantined")

###############################################################################
# 4. ENRIQUECIMIENTO: CRUCE STREAM-STREAM Y WATERMARKS (Punto 3.5.4)
###############################################################################

# REQUISITO 3.5.4: Umbral de espera estimado empíricamente (30 días para cubrir 
# el ciclo de datos Jan-Mar observado en vuestros archivos Bronce).
events_wait = "30 days"
labels_wait = "24 hours"

@dp.table(
    name = "silver_abandonment_events", 
    comment = "Tabla unificada que cruza navegación con etiquetas usando Watermarks."
)
def silver_events_join():
    """
    Une flujos continuos gestionando la memoria de estado de Spark.
    """
    df_ev = (spark.readStream.table("vw_clean_cart_events")
             .withWatermark("event_timestamp", events_wait))
    
    df_lb = (spark.readStream.table("vw_clean_labels")
             .withWatermark("label_available_date", labels_wait))

    # Join LEFT para conservar eventos incluso sin etiqueta inmediata
    return df_ev.alias("ev").join(
        df_lb.alias("lbl"),
        on = [
            col("ev.session_id") == col("lbl.session_id"),
            col("lbl.label_available_date") >= col("ev.event_timestamp"),
            col("lbl.label_available_date") <= col("ev.event_timestamp") + expr(f"INTERVAL {events_wait}")
        ],
        how = "leftOuter"
    ).select(
        col("ev.*"), 
        col("lbl.will_abandon"), 
        col("lbl.label_available_date")
    )