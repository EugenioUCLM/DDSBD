"""
The purpose of this script is to establish the first layer of our data
architecture (`Medallion` architecture) known as the `Bronze` layer.
It handles the ingestion of raw files from the landing zone into managed
`Delta` tables using `Databricks Lakeflow Declarative Pipelines`.

It covers batch ingestion for master data (customers) and continuous
streaming ingestion via `Auto Loader` for events (transactions and labels).
"""


###############################################################################
# Imports
###############################################################################

# Object-oriented filesystem paths
from pathlib import Path

# Declarative pipelines
import pyspark.pipelines as dp

# Functions for metadata
from pyspark.sql.functions import col, current_timestamp


###############################################################################
# Configuration variables and constants
###############################################################################

base_path = Path("/") / "Volumes" / "workspace" / "abandono_carrito_comercio_electronico"
vol_landing_zone = base_path / "landing_zone"


###############################################################################
# Customer ingestion (batch)
###############################################################################

customers_table_name = "bronze_customers"
customers_comment = """
This managed table belongs to the **`bronze`** layer and stores customer
via batch loads using a full refresh strategy. It contains the raw information
master data (context). It acts as a static snapshot of user profiles and is
updated  extracted from the landing zone, serving as the static foundation to
enrich transactional events.
"""

path_context = vol_landing_zone / "context"


@dp.table(name = customers_table_name, comment = customers_comment)
def bronze_customers():
    """
    We use a standard batch read because customer data is a snapshot.

    The framework automatically handles the "overwrite" (full refresh) logic.
    """
    return (
        spark.read
             .format("csv")
             .option("header", "true")  # The first row contains column names
             .option("inferSchema", "true")  # Automatically infer schema
             .load(str(path_context))  # Load from the volume

             # Add technical metadata columns for data lineage and auditing
             .withColumn("ingestion_timestamp", current_timestamp())
             .withColumn("source_file", col("_metadata.file_path"))
    )


###############################################################################
# Events ingestion: transactions (streaming)
###############################################################################

tx_table_name = "bronze_transactions"
tx_comment = """
This managed table acts as the historical and real-time repository for raw
financial events within the **`bronze`** layer. It is continuously fed by
a streaming pipeline (`Auto Loader`), capturing every credit card transaction
in an append-only format. It preserves the original `.json` schema and includes
technical audit metadata (`ingestion_timestamp`, `source_file`) to ensure data
lineage and traceability.
"""

path_events_tx = vol_landing_zone / "events" / "transactions"


# The decorator automatically infers that this is a streaming table.
# It instructs the engine to continuously append new records read by
# the auto loader to the end of the table, without modifying or overwriting
# any existing data.
@dp.table(name = tx_table_name, comment = tx_comment)
def bronze_tx_flow():
    """
    Reads the transaction stream using `Auto Loader` (`cloudFiles`).

    It acts as a continuous sensor to ingest new `.json` files as they
    arrive.
    """
    return (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "json")
             .option("cloudFiles.inferColumnTypes", "true")
             .load(str(path_events_tx))
             .withColumn("ingestion_timestamp", current_timestamp())
             .withColumn("source_file", col("_metadata.file_path"))
    )


###############################################################################
# Events ingestion: fraud labels (streaming)
###############################################################################

labels_table_name = "bronze_labels"
labels_comment = """
This managed **`bronze` layer** table captures the delayed feedback (labels)
regarding the legitimacy of the operations. It is asynchronously fed by a
streaming pipeline (`Auto Loader`) as the fraud status of a purchase is
confirmed. It maintains the original raw format and includes lineage metadata,
setting the stage for an exact `JOIN` with `bronze_transactions` in downstream
layers.
"""

path_events_labels = vol_landing_zone / "events" / "labels"


@dp.table(name = labels_table_name, comment = labels_comment)
def bronze_labels_flow():
    """
    Reads the delayed fraud labels stream using `Auto Loader` (`cloudFiles`).

    Continuously ingests new feedback `.json` files from the landing zone.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(str(path_events_labels))
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
    )
