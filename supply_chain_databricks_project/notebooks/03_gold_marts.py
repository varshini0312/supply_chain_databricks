# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold Business Marts
# MAGIC Analytics-ready tables for BI / dashboards.
# MAGIC
# MAGIC | Table | Grain | Answers |
# MAGIC |---|---|---|
# MAGIC | `fact_sales` | order line | Revenue, margin, volume by product / customer / warehouse |
# MAGIC | `mart_supplier_performance` | supplier x month | On-time-in-full (OTIF), fill rate, lead-time vs promise |
# MAGIC | `mart_shipment_performance` | carrier x mode x month | On-time %, avg delay, cost per unit |
# MAGIC | `mart_inventory_health` | warehouse x product (latest week) | Days of cover, stock-out & overstock flags |
# MAGIC | `mart_warehouse_kpis` | warehouse x month | Throughput, revenue, capacity utilisation |

# COMMAND ----------

from pyspark.sql import functions as F, Window

dbutils.widgets.text("catalog", "supply_chain", "Catalog")
catalog = dbutils.widgets.get("catalog")
S, G = f"{catalog}.silver", f"{catalog}.gold"

def save(df, name, partition=None):
    w = df.write.format("delta").mode("overwrite").option("overwriteSchema", True)
    if partition:
        w = w.partitionBy(partition)
    w.saveAsTable(f"{G}.{name}")
    print(f"{name}: {df.count():,} rows")

sales = spark.table(f"{S}.sales_orders")
products = spark.table(f"{S}.products")
customers = spark.table(f"{S}.customers")
warehouses = spark.table(f"{S}.warehouses")
suppliers = spark.table(f"{S}.suppliers")

# COMMAND ----------

# MAGIC %md ## fact_sales

# COMMAND ----------

fact_sales = (
    sales.filter(F.col("order_status").isin("Delivered", "Shipped"))   # exclude cancelled / returned from revenue
    .join(products.select("product_id", "sku", "product_name", "category", "subcategory", "unit_cost"), "product_id")
    .join(customers.select("customer_id", "customer_name", "segment", "region"), "customer_id")
    .join(warehouses.select("warehouse_id", "warehouse_name", F.col("country").alias("warehouse_country")), "warehouse_id")
    .withColumn("cogs", F.round(F.col("quantity") * F.col("unit_cost"), 2))
    .withColumn("gross_margin", F.round(F.col("net_revenue") - F.col("cogs"), 2))
    .withColumn("gross_margin_pct", F.round(F.col("gross_margin") / F.col("net_revenue"), 4))
    .withColumn("order_year", F.year("order_date"))
    .withColumn("order_month", F.date_trunc("month", "order_date").cast("date"))
)
save(fact_sales, "fact_sales", "order_year")

# COMMAND ----------

# MAGIC %md ## mart_supplier_performance

# COMMAND ----------

po = spark.table(f"{S}.purchase_orders").filter("po_status = 'Received'")
mart_supplier = (
    po.withColumn("month", F.date_trunc("month", "po_date").cast("date"))
    .withColumn("on_time", (F.col("delay_days") <= 0).cast("int"))
    .withColumn("in_full", (F.col("quantity_received") >= F.col("quantity_ordered")).cast("int"))
    .withColumn("otif", (F.col("on_time") * F.col("in_full")))
    .groupBy("supplier_id", "month")
    .agg(
        F.count("*").alias("po_count"),
        F.round(F.avg("on_time"), 4).alias("on_time_rate"),
        F.round(F.avg("in_full"), 4).alias("in_full_rate"),
        F.round(F.avg("otif"), 4).alias("otif_rate"),
        F.round(F.avg("fill_rate"), 4).alias("avg_fill_rate"),
        F.round(F.avg("lead_time_days"), 1).alias("avg_lead_time_days"),
        F.round(F.avg("delay_days"), 2).alias("avg_delay_days"),
        F.round(F.sum("po_value"), 2).alias("total_spend"),
    )
    .join(suppliers.select("supplier_id", "supplier_name", "country", "tier"), "supplier_id")
)
# 3-month rolling OTIF for trend analysis
w3 = Window.partitionBy("supplier_id").orderBy("month").rowsBetween(-2, 0)
mart_supplier = mart_supplier.withColumn("otif_rolling_3m", F.round(F.avg("otif_rate").over(w3), 4))
save(mart_supplier, "mart_supplier_performance")

# COMMAND ----------

# MAGIC %md ## mart_shipment_performance

# COMMAND ----------

ship = spark.table(f"{S}.shipments").filter("shipment_status = 'Delivered'")
sales_qty = sales.select("order_id", "quantity")
mart_ship = (
    ship.join(sales_qty, "order_id")
    .withColumn("month", F.date_trunc("month", "ship_date").cast("date"))
    .groupBy("carrier", "shipping_mode", "month")
    .agg(
        F.count("*").alias("shipments"),
        F.round(F.avg(F.col("is_on_time").cast("int")), 4).alias("on_time_rate"),
        F.round(F.avg("delay_days"), 2).alias("avg_delay_days"),
        F.round(F.sum("shipping_cost"), 2).alias("total_shipping_cost"),
        F.round(F.sum("shipping_cost") / F.sum("quantity"), 4).alias("cost_per_unit"),
    )
)
save(mart_ship, "mart_shipment_performance")

# COMMAND ----------

# MAGIC %md ## mart_inventory_health

# COMMAND ----------

inv = spark.table(f"{S}.inventory_snapshots")
latest = inv.agg(F.max("snapshot_date")).first()[0]
cutoff = F.lit(latest)

# average weekly demand over the trailing 12 weeks (from sales)
weeks = 12
demand = (
    sales.filter(F.col("order_status").isin("Delivered", "Shipped"))
    .filter(F.col("order_date") > F.date_sub(cutoff, weeks * 7))
    .groupBy("warehouse_id", "product_id")
    .agg((F.sum("quantity") / weeks).alias("avg_weekly_demand"))
)
mart_inv = (
    inv.filter(F.col("snapshot_date") == cutoff)
    .join(demand, ["warehouse_id", "product_id"], "left")
    .join(products.select("product_id", "sku", "product_name", "category", "reorder_point"), "product_id")
    .join(warehouses.select("warehouse_id", "warehouse_name"), "warehouse_id")
    .withColumn("avg_weekly_demand", F.coalesce("avg_weekly_demand", F.lit(0.0)))
    .withColumn("weeks_of_cover",
                F.when(F.col("avg_weekly_demand") > 0, F.round(F.col("quantity_available") / F.col("avg_weekly_demand"), 1)))
    .withColumn("stock_status",
                F.when(F.col("quantity_available") <= 0, "Stock-out")
                 .when(F.col("quantity_available") < F.col("reorder_point"), "Below Reorder Point")
                 .when(F.col("weeks_of_cover") > 26, "Overstock")
                 .otherwise("Healthy"))
)
save(mart_inv, "mart_inventory_health")

# COMMAND ----------

# MAGIC %md ## mart_warehouse_kpis

# COMMAND ----------

mart_wh = (
    spark.table(f"{G}.fact_sales")
    .groupBy("warehouse_id", "warehouse_name", "order_month")
    .agg(
        F.countDistinct("order_id").alias("orders"),
        F.sum("quantity").alias("units_shipped"),
        F.round(F.sum("net_revenue"), 2).alias("net_revenue"),
        F.round(F.sum("gross_margin"), 2).alias("gross_margin"),
    )
    .join(warehouses.select("warehouse_id", "capacity_units"), "warehouse_id")
    .withColumn("throughput_vs_capacity_pct", F.round(F.col("units_shipped") / F.col("capacity_units"), 4))
)
save(mart_wh, "mart_warehouse_kpis")

# COMMAND ----------

# Optional: table & column documentation for Unity Catalog discoverability
spark.sql(f"COMMENT ON TABLE {G}.fact_sales IS 'Order-line sales fact (delivered/shipped only) with margin metrics'")
spark.sql(f"COMMENT ON TABLE {G}.mart_supplier_performance IS 'Supplier OTIF, fill rate and lead-time KPIs by month'")
spark.sql(f"COMMENT ON TABLE {G}.mart_inventory_health IS 'Latest weekly inventory position with weeks-of-cover and stock status'")
