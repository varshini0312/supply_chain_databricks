# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup & Configuration
# MAGIC Creates the catalog objects used by the Supply Chain Lakehouse (medallion architecture).
# MAGIC
# MAGIC **Layers:** `bronze` (raw, append-only) -> `silver` (cleansed, conformed) -> `gold` (business marts)
# MAGIC
# MAGIC **Before running:** upload the CSVs from `data/raw/` to the Unity Catalog Volume
# MAGIC `/Volumes/<catalog>/bronze/raw_landing/` (see README).

# COMMAND ----------

dbutils.widgets.text("catalog", "supply_chain", "Catalog")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
for layer in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{layer}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.bronze.raw_landing")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.ops")  # DQ results / audit tables

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN {catalog}"))
print(f"Upload raw CSVs to: /Volumes/{catalog}/bronze/raw_landing/")
