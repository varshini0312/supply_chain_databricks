# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze Ingestion
# MAGIC Loads raw CSVs **as-is** (all columns as strings, no cleansing) into Delta tables and
# MAGIC stamps lineage metadata: `_ingested_at`, `_source_file`, `_batch_id`.
# MAGIC
# MAGIC Design choices
# MAGIC - Strings only in Bronze -> bad values never break ingestion; typing happens in Silver.
# MAGIC - `mergeSchema` enabled so new upstream columns don't fail the pipeline.
# MAGIC - Overwrite per batch keeps the demo idempotent; for production swap to Auto Loader (snippet at the bottom).

# COMMAND ----------

import uuid
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "supply_chain", "Catalog")
catalog = dbutils.widgets.get("catalog")
landing = f"/Volumes/{catalog}/bronze/raw_landing"
batch_id = str(uuid.uuid4())

TABLES = [
    "suppliers", "products", "warehouses", "customers",
    "purchase_orders", "sales_orders", "shipments", "inventory_snapshots",
]

# COMMAND ----------

def ingest(table: str) -> int:
    df = (
        spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", False)          # keep everything as string in Bronze
        .option("multiLine", False)
        .load(f"{landing}/{table}.csv")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_batch_id", F.lit(batch_id))
    )
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .saveAsTable(f"{catalog}.bronze.{table}")
    )
    return df.count()

results = [(t, ingest(t)) for t in TABLES]
display(spark.createDataFrame(results, ["table", "rows_ingested"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Production variant - Auto Loader (incremental, exactly-once)
# MAGIC ```python
# MAGIC (spark.readStream.format("cloudFiles")
# MAGIC     .option("cloudFiles.format", "csv")
# MAGIC     .option("header", True)
# MAGIC     .option("cloudFiles.schemaLocation", f"{landing}/_schemas/sales_orders")
# MAGIC     .load(f"{landing}/sales_orders/")
# MAGIC     .writeStream
# MAGIC     .option("checkpointLocation", f"{landing}/_checkpoints/sales_orders")
# MAGIC     .trigger(availableNow=True)
# MAGIC     .toTable(f"{catalog}.bronze.sales_orders"))
# MAGIC ```
