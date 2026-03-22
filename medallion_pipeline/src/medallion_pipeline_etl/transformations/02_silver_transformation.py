"""
Capa Silver - Proyecto Abandono de Carrito.
Implementación de arquitectura de Cuarentena (DLQ) y AUTO CDC según 
los requisitos del punto 3.5.2 de la guía docente.
"""

import pyspark.pipelines as dp
from pyspark.sql.functions import coalesce, col, expr, to_timestamp
from rules import get_rules

###############################################################################
# 1. DIMENSIÓN USUARIOS (Cuarentena + AUTO CDC)
###############################################################################

# REQUISITO 1: Crear tabla física de cuarentena de forma anticipada
cust_quarantine_table = "silver_quarantine_customers"
dp.create_streaming_table(
    name = cust_quarantine_table,
    comment = "DLQ de usuarios: Almacena registros que fallan reglas de calidad."
)

cust_rules_dict = get_rules("customers")
cust_quarantine_expr = "NOT (" + " AND ".join(cust_rules_dict.values()) + ")"

# REQUISITO 2: Tabla temporal como hub de enrutamiento con is_quarantined
@dp.table(name = "tmp_eval_customers", temporary = True)
@dp.expect_all(cust_rules_dict)
def eval_customers():
    return (
        spark.readStream
             .option("skipChangeCommits", "true") # REQUISITO 2: Opción específica
             .table("bronze_customers")
             .withColumn("user_updated_at", coalesce(col("user_updated_at"), col("registration_date")))
             .withColumn("is_quarantined", expr(cust_quarantine_expr))
    )

# REQUISITO 3: Bifurcación (Invalid -> append_flow / Valid -> view)
@dp.append_flow(target = cust_quarantine_table, name = "flow_quarantine_cust")
def quarantine_customers():
    return spark.readStream.table("tmp_eval_customers").filter("is_quarantined = true").drop("is_quarantined")

@dp.view(name = "vw_clean_customers")
def clean_customers():
    return spark.readStream.table("tmp_eval_customers").filter("is_quarantined = false").drop("is_quarantined")

# REQUISITO 4: AUTO CDC desde la vista limpia
dp.create_streaming_table(name = "silver_users_history", comment = "SCD Tipo 2 de usuarios.")
dp.create_auto_cdc_flow(
    target = "silver_users_history",
    source = "vw_clean_customers",
    keys = ["user_id"],
    sequence_by = col("user_updated_at"),
    except_column_list = ["ingestion_timestamp", "source_file"],
    stored_as_scd_type = "2"
)

###############################################################################
# 2. EVENTOS DE CARRITO (Cuarentena + Streaming)
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
# 3. ETIQUETAS DE ABANDONO (Cuarentena + Streaming)
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
# 4. UNIÓN MAESTRA (Stream-Stream Join)
###############################################################################

@dp.table(name = "silver_abandonment_events")
def silver_events_join():
    df_ev = spark.readStream.table("vw_clean_cart_events").withWatermark("event_timestamp", "24 hours")
    df_lb = spark.readStream.table("vw_clean_labels").withWatermark("label_available_date", "1 hour")

    return df_ev.alias("ev").join(
        df_lb.alias("lbl"),
        on = [
            col("ev.session_id") == col("lbl.session_id"),
            col("lbl.label_available_date") >= col("ev.event_timestamp"),
            col("lbl.label_available_date") <= col("ev.event_timestamp") + expr("INTERVAL 24 hours")
        ],
        how = "leftOuter"
    ).select(col("ev.*"), col("lbl.will_abandon"), col("lbl.label_available_date"))