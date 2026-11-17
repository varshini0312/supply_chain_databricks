# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver Transformations
# MAGIC Cleanses and conforms Bronze data: type casting, trimming/standardising text, de-duplication,
# MAGIC business-rule validation. Rows failing a rule are written to `silver.<table>_quarantine`
# MAGIC with the failure reason instead of being silently dropped.
# MAGIC
# MAGIC | Table | Issues handled |
# MAGIC |---|---|
# MAGIC | sales_orders | duplicates, null discount -> 0, negative qty, orphan product FK |
# MAGIC | shipments | status casing/whitespace, carrier casing, ship date before order date |
# MAGIC | purchase_orders | received qty > ordered qty |

# COMMAND ----------

from itertools import chain
from pyspark.sql import functions as F, Window

dbutils.widgets.text("catalog", "supply_chain", "Catalog")
catalog = dbutils.widgets.get("catalog")
B, S = f"{catalog}.bronze", f"{catalog}.silver"

# Canonical carrier names (raw feed mixes 'dhl' / 'DHL' etc.)
CARRIERS = {"dhl": "DHL", "fedex": "FedEx", "ups": "UPS", "maersk": "Maersk", "bluedart": "BlueDart"}
carrier_map = F.create_map(*[F.lit(x) for x in chain.from_iterable(CARRIERS.items())])

def write_silver(df, name, quarantine=None):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{S}.{name}")
    msg = f"{name}: {df.count():,} rows"
    if quarantine is not None:
        quarantine.write.format("delta").mode("overwrite").option("overwriteSchema", True)\
            .saveAsTable(f"{S}.{name}_quarantine")
        msg += f" | quarantined: {quarantine.count():,}"
    print(msg)

def split_valid(df, rules: dict):
    """rules: {reason: boolean Column that is TRUE when the row is VALID}. Returns (valid, quarantine)."""
    reason = F.concat_ws("; ", *[F.when(~F.coalesce(cond, F.lit(False)), F.lit(r)) for r, cond in rules.items()])
    flagged = df.withColumn("_dq_failures", reason)
    valid = flagged.filter(F.col("_dq_failures") == "").drop("_dq_failures")
    bad = flagged.filter(F.col("_dq_failures") != "")
    return valid, bad

# COMMAND ----------

# MAGIC %md ## Reference / dimension tables

# COMMAND ----------

suppliers = (
    spark.table(f"{B}.suppliers")
    .select(
        "supplier_id",
        F.trim("supplier_name").alias("supplier_name"),
        F.trim("country").alias("country"), F.trim("city").alias("city"),
        F.trim("tier").alias("tier"),
        F.col("avg_lead_time_days").cast("int"),
        F.col("reliability_score").cast("double"),
        F.col("payment_terms"),
        F.to_date("onboarded_date").alias("onboarded_date"),
        F.lower("is_active").cast("boolean").alias("is_active"),
    ).dropDuplicates(["supplier_id"])
)
write_silver(suppliers, "suppliers")

products = (
    spark.table(f"{B}.products")
    .select(
        "product_id", "sku", F.trim("product_name").alias("product_name"),
        "category", "subcategory",
        F.col("unit_cost").cast("decimal(12,2)"), F.col("unit_price").cast("decimal(12,2)"),
        F.col("weight_kg").cast("double"), "supplier_id",
        F.col("shelf_life_days").cast("int"), F.col("reorder_point").cast("int"),
    ).dropDuplicates(["product_id"])
)
write_silver(products, "products")

warehouses = (
    spark.table(f"{B}.warehouses")
    .select("warehouse_id", "warehouse_name", "city", "state_region", "country",
            F.col("capacity_units").cast("int"), "warehouse_type")
    .dropDuplicates(["warehouse_id"])
)
write_silver(warehouses, "warehouses")

customers = (
    spark.table(f"{B}.customers")
    .select("customer_id", F.trim("customer_name").alias("customer_name"), "segment", "region", "country",
            F.to_date("signup_date").alias("signup_date"))
    .dropDuplicates(["customer_id"])
)
write_silver(customers, "customers")

# COMMAND ----------

# MAGIC %md ## sales_orders

# COMMAND ----------

sales_raw = (
    spark.table(f"{B}.sales_orders")
    .select(
        "order_id", F.to_date("order_date").alias("order_date"), "customer_id", "product_id", "warehouse_id",
        F.col("quantity").cast("int"), F.col("unit_price").cast("decimal(12,2)"),
        F.coalesce(F.col("discount_pct").cast("double"), F.lit(0.0)).alias("discount_pct"),  # null -> no discount
        F.initcap(F.trim("order_status")).alias("order_status"),
    )
)

# De-duplicate on business key (keep first occurrence)
w = Window.partitionBy("order_id").orderBy("order_date")
sales_dedup = sales_raw.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

valid_products = spark.table(f"{S}.products").select("product_id").withColumn("_fk_ok", F.lit(True))
sales_chk = sales_dedup.join(valid_products, "product_id", "left")

sales_valid, sales_bad = split_valid(sales_chk, {
    "quantity_not_positive": F.col("quantity") > 0,
    "unknown_product_id": F.col("_fk_ok").isNotNull(),
    "discount_out_of_range": F.col("discount_pct").between(0, 1),
})
sales_valid = sales_valid.drop("_fk_ok").withColumn(
    "net_revenue", F.round(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount_pct")), 2)
)
write_silver(sales_valid, "sales_orders", sales_bad.drop("_fk_ok"))

# COMMAND ----------

# MAGIC %md ## shipments

# COMMAND ----------

ship = (
    spark.table(f"{B}.shipments")
    .select(
        "shipment_id", "order_id", "warehouse_id",
        F.coalesce(carrier_map[F.lower(F.trim("carrier"))], F.trim("carrier")).alias("carrier"),
        F.initcap(F.trim("shipping_mode")).alias("shipping_mode"),
        F.to_date("ship_date").alias("ship_date"),
        F.to_date("expected_delivery_date").alias("expected_delivery_date"),
        F.to_date("actual_delivery_date").alias("actual_delivery_date"),
        F.col("shipping_cost").cast("decimal(12,2)"),
        F.initcap(F.trim("shipment_status")).alias("shipment_status"),   # 'DELIVERED ' -> 'Delivered'
    ).dropDuplicates(["shipment_id"])
)
orders_dates = spark.table(f"{S}.sales_orders").select("order_id", "order_date")
ship_chk = ship.join(orders_dates, "order_id", "left")

ship_valid, ship_bad = split_valid(ship_chk, {
    "ship_before_order": F.col("ship_date") >= F.col("order_date"),
    "delivery_before_ship": F.col("actual_delivery_date").isNull() | (F.col("actual_delivery_date") >= F.col("ship_date")),
    "order_not_in_silver": F.col("order_date").isNotNull(),
})
ship_valid = (
    ship_valid.drop("order_date")
    .withColumn("is_on_time", F.when(F.col("actual_delivery_date").isNull(), None)
                .otherwise(F.col("actual_delivery_date") <= F.col("expected_delivery_date")))
    .withColumn("delay_days", F.when(F.col("actual_delivery_date").isNotNull(),
                F.datediff("actual_delivery_date", "expected_delivery_date")))
)
write_silver(ship_valid, "shipments", ship_bad)

# COMMAND ----------

# MAGIC %md ## purchase_orders

# COMMAND ----------

po = (
    spark.table(f"{B}.purchase_orders")
    .select(
        "po_id", F.to_date("po_date").alias("po_date"), "supplier_id", "product_id", "warehouse_id",
        F.col("quantity_ordered").cast("int"), F.col("unit_cost").cast("decimal(12,2)"),
        F.to_date("expected_delivery_date").alias("expected_delivery_date"),
        F.to_date("actual_delivery_date").alias("actual_delivery_date"),
        F.col("quantity_received").cast("int"), F.initcap(F.trim("po_status")).alias("po_status"),
    ).dropDuplicates(["po_id"])
)
po_valid, po_bad = split_valid(po, {
    "received_exceeds_ordered": F.col("quantity_received") <= F.col("quantity_ordered"),
    "non_positive_order_qty": F.col("quantity_ordered") > 0,
})
po_valid = (
    po_valid
    .withColumn("lead_time_days", F.datediff("actual_delivery_date", "po_date"))
    .withColumn("delay_days", F.datediff("actual_delivery_date", "expected_delivery_date"))
    .withColumn("fill_rate", F.when(F.col("po_status") == "Received",
                F.round(F.col("quantity_received") / F.col("quantity_ordered"), 4)))
    .withColumn("po_value", F.round(F.col("quantity_ordered") * F.col("unit_cost"), 2))
)
write_silver(po_valid, "purchase_orders", po_bad)

# COMMAND ----------

# MAGIC %md ## inventory_snapshots

# COMMAND ----------

inv = (
    spark.table(f"{B}.inventory_snapshots")
    .select(
        F.to_date("snapshot_date").alias("snapshot_date"), "warehouse_id", "product_id",
        F.col("quantity_on_hand").cast("int"), F.col("quantity_reserved").cast("int"),
        F.col("quantity_on_order").cast("int"),
    ).dropDuplicates(["snapshot_date", "warehouse_id", "product_id"])
    .withColumn("quantity_available", F.col("quantity_on_hand") - F.col("quantity_reserved"))
)
write_silver(inv, "inventory_snapshots")
