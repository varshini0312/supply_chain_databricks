# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Data Quality Checks
# MAGIC Runs assertions across layers and logs the outcome to `ops.dq_results`.
# MAGIC The notebook **fails the job** if any `error`-severity check fails, so bad data never reaches consumers.

# COMMAND ----------

from datetime import datetime
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "supply_chain", "Catalog")
catalog = dbutils.widgets.get("catalog")
run_ts = datetime.utcnow()
results = []

def check(name, layer, table, passed, detail, severity="error"):
    results.append((run_ts, layer, table, name, bool(passed), str(detail), severity))

def q(sql): return spark.sql(sql).first()[0]

# COMMAND ----------

# 1. Row-count reconciliation: Bronze vs Silver (+ quarantine) for tables with quarantine
for t in ["sales_orders", "shipments", "purchase_orders"]:
    b = spark.table(f"{catalog}.bronze.{t}").count()
    s = spark.table(f"{catalog}.silver.{t}").count()
    qn = spark.table(f"{catalog}.silver.{t}_quarantine").count()
    # Bronze may contain duplicates that Silver removes, so Silver + quarantine must be <= Bronze
    check("row_reconciliation", "silver", t, s + qn <= b, f"bronze={b} silver={s} quarantine={qn}")

# 2. Primary key uniqueness
for t, key in [("sales_orders", "order_id"), ("shipments", "shipment_id"), ("purchase_orders", "po_id"),
               ("products", "product_id"), ("suppliers", "supplier_id")]:
    dups = q(f"SELECT COUNT(*) - COUNT(DISTINCT {key}) FROM {catalog}.silver.{t}")
    check("pk_unique", "silver", t, dups == 0, f"duplicate_keys={dups}")

# 3. Not-null critical columns
for t, cols in {"sales_orders": ["order_id", "order_date", "customer_id", "product_id", "quantity"],
                "shipments": ["shipment_id", "order_id", "ship_date"]}.items():
    for c in cols:
        n = q(f"SELECT COUNT(*) FROM {catalog}.silver.{t} WHERE {c} IS NULL")
        check(f"not_null_{c}", "silver", t, n == 0, f"null_rows={n}")

# 4. Referential integrity
fk = [("sales_orders", "customer_id", "customers", "customer_id"),
      ("sales_orders", "product_id", "products", "product_id"),
      ("sales_orders", "warehouse_id", "warehouses", "warehouse_id"),
      ("purchase_orders", "supplier_id", "suppliers", "supplier_id"),
      ("shipments", "order_id", "sales_orders", "order_id")]
for ct, cc, pt, pc in fk:
    orphans = q(f"""SELECT COUNT(*) FROM {catalog}.silver.{ct} c
                    LEFT JOIN {catalog}.silver.{pt} p ON c.{cc} = p.{pc} WHERE p.{pc} IS NULL""")
    check(f"fk_{cc}->{pt}", "silver", ct, orphans == 0, f"orphans={orphans}")

# 5. Domain / range rules
check("positive_quantity", "silver", "sales_orders",
      q(f"SELECT COUNT(*) FROM {catalog}.silver.sales_orders WHERE quantity <= 0") == 0, "quantity > 0")
check("status_values", "silver", "shipments",
      q(f"""SELECT COUNT(*) FROM {catalog}.silver.shipments
            WHERE shipment_status NOT IN ('Delivered','In Transit')""") == 0, "clean status domain")

# 6. Gold sanity
rev_silver = q(f"SELECT SUM(net_revenue) FROM {catalog}.silver.sales_orders WHERE order_status IN ('Delivered','Shipped')")
rev_gold = q(f"SELECT SUM(net_revenue) FROM {catalog}.gold.fact_sales")
check("revenue_silver_vs_gold", "gold", "fact_sales", abs(rev_silver - rev_gold) < 1, f"silver={rev_silver} gold={rev_gold}")

# 7. Freshness (warning only)
latest = q(f"SELECT MAX(order_date) FROM {catalog}.silver.sales_orders")
check("freshness_info", "silver", "sales_orders", True, f"latest_order_date={latest}", severity="info")

# COMMAND ----------

schema = "run_ts timestamp, layer string, table_name string, check_name string, passed boolean, detail string, severity string"
dq = spark.createDataFrame(results, schema)
dq.write.format("delta").mode("append").saveAsTable(f"{catalog}.ops.dq_results")
display(dq.orderBy("passed", "layer"))

failed = dq.filter("passed = false AND severity = 'error'").count()
if failed:
    raise Exception(f"{failed} data quality check(s) failed - see {catalog}.ops.dq_results")
print("All blocking data-quality checks passed.")
