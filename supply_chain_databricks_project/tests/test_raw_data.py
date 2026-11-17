"""Local sanity tests on the raw CSVs (run: pytest -q). No Spark required."""
from pathlib import Path
import pandas as pd
import pytest

RAW = Path(__file__).resolve().parents[1] / "data" / "raw"
T = {p.stem: pd.read_csv(p) for p in RAW.glob("*.csv")}


def test_all_tables_present():
    expected = {"suppliers", "products", "warehouses", "customers",
                "purchase_orders", "sales_orders", "shipments", "inventory_snapshots"}
    assert expected == set(T)


@pytest.mark.parametrize("table,key", [
    ("suppliers", "supplier_id"), ("products", "product_id"), ("warehouses", "warehouse_id"),
    ("customers", "customer_id"), ("purchase_orders", "po_id"), ("shipments", "shipment_id"),
])
def test_primary_keys_unique(table, key):
    assert T[table][key].is_unique


@pytest.mark.parametrize("child,col,parent,pcol", [
    ("products", "supplier_id", "suppliers", "supplier_id"),
    ("purchase_orders", "supplier_id", "suppliers", "supplier_id"),
    ("purchase_orders", "warehouse_id", "warehouses", "warehouse_id"),
    ("sales_orders", "customer_id", "customers", "customer_id"),
    ("sales_orders", "warehouse_id", "warehouses", "warehouse_id"),
    ("shipments", "order_id", "sales_orders", "order_id"),
    ("inventory_snapshots", "product_id", "products", "product_id"),
])
def test_foreign_keys_clean(child, col, parent, pcol):
    assert set(T[child][col]).issubset(set(T[parent][pcol]))


def test_known_dirty_data_is_present():
    """The raw layer must contain the documented issues so Silver has work to do."""
    s = T["sales_orders"]
    assert s["order_id"].duplicated().sum() > 0           # duplicates
    assert s["discount_pct"].isna().sum() > 0             # null discounts
    assert (s["quantity"] <= 0).sum() > 0                 # negative quantities
    assert (~s["product_id"].isin(T["products"]["product_id"])).sum() > 0   # orphan FK
    sh = T["shipments"]
    assert (sh["shipment_status"] != sh["shipment_status"].str.strip().str.title()).sum() > 0
    po = T["purchase_orders"]
    assert (po["quantity_received"] > po["quantity_ordered"]).sum() > 0


def test_date_ranges():
    d = pd.to_datetime(T["sales_orders"]["order_date"])
    assert d.min() >= pd.Timestamp("2024-01-01") and d.max() <= pd.Timestamp("2025-12-31")
