"""
Synthetic supply chain data generator.

Produces 8 relational CSV files under data/raw/ that mimic the extracts a
company would receive from ERP / WMS / TMS systems. A small, documented amount
of "dirty" data is injected into the transactional tables so the Silver layer
has real cleansing and data-quality work to do.

Usage:
    python src/generate_data.py --out data/raw --seed 42
"""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

START = pd.Timestamp("2024-01-01")
END = pd.Timestamp("2025-12-31")


def build_suppliers(rng: np.random.Generator, n: int = 60) -> pd.DataFrame:
    countries = {
        "China": ["Shenzhen", "Shanghai", "Guangzhou"],
        "India": ["Chennai", "Pune", "Bengaluru"],
        "Germany": ["Munich", "Hamburg"],
        "Vietnam": ["Ho Chi Minh City", "Hanoi"],
        "Mexico": ["Monterrey", "Guadalajara"],
        "United States": ["Dallas", "Chicago", "Atlanta"],
        "Poland": ["Warsaw", "Gdansk"],
        "Turkey": ["Istanbul", "Izmir"],
    }
    rows = []
    for i in range(1, n + 1):
        country = rng.choice(list(countries))
        tier = rng.choice(["Strategic", "Preferred", "Approved"], p=[0.2, 0.4, 0.4])
        base_lt = {"China": 28, "India": 21, "Germany": 10, "Vietnam": 26,
                   "Mexico": 7, "United States": 5, "Poland": 9, "Turkey": 12}[country]
        rows.append({
            "supplier_id": f"SUP{i:04d}",
            "supplier_name": f"{rng.choice(['Apex','Nova','Prime','Vertex','Orion','Zenith','Atlas','Helix','Summit','Pinnacle'])} "
                             f"{rng.choice(['Components','Industries','Logistics','Manufacturing','Supply','Trading'])} {i}",
            "country": country,
            "city": rng.choice(countries[country]),
            "tier": tier,
            "avg_lead_time_days": int(max(2, rng.normal(base_lt, 3))),
            "reliability_score": round(float(np.clip(rng.normal(0.88 if tier != "Approved" else 0.8, 0.07), 0.5, 0.99)), 3),
            "payment_terms": rng.choice(["Net30", "Net45", "Net60", "Net90"], p=[0.35, 0.35, 0.2, 0.1]),
            "onboarded_date": (START - pd.Timedelta(days=int(rng.integers(90, 2500)))).date().isoformat(),
            "is_active": bool(rng.random() > 0.05),
        })
    return pd.DataFrame(rows)


def build_products(rng: np.random.Generator, suppliers: pd.DataFrame, n: int = 120) -> pd.DataFrame:
    catalog = {
        "Electronics": ["Sensors", "Controllers", "Cables", "Batteries"],
        "Industrial": ["Bearings", "Fasteners", "Valves", "Motors"],
        "Packaging": ["Cartons", "Pallets", "Wrap", "Labels"],
        "Consumer Goods": ["Home", "Kitchen", "Outdoor", "Office"],
        "Healthcare": ["Disposables", "Diagnostics", "PPE"],
    }
    cost_range = {"Electronics": (8, 180), "Industrial": (5, 250), "Packaging": (0.5, 20),
                  "Consumer Goods": (3, 90), "Healthcare": (1, 60)}
    rows = []
    for i in range(1, n + 1):
        cat = rng.choice(list(catalog))
        sub = rng.choice(catalog[cat])
        lo, hi = cost_range[cat]
        cost = round(float(np.exp(rng.uniform(np.log(lo), np.log(hi)))), 2)
        rows.append({
            "product_id": f"PRD{i:05d}",
            "sku": f"{cat[:3].upper()}-{sub[:3].upper()}-{i:04d}",
            "product_name": f"{sub} {rng.choice(['Pro','Lite','Max','Standard','Plus'])} {i}",
            "category": cat,
            "subcategory": sub,
            "unit_cost": cost,
            "unit_price": round(cost * float(rng.uniform(1.25, 1.9)), 2),
            "weight_kg": round(float(rng.uniform(0.05, 25)), 2),
            "supplier_id": rng.choice(suppliers["supplier_id"]),
            "shelf_life_days": int(rng.choice([0, 180, 365, 730], p=[0.6, 0.1, 0.2, 0.1])),
            "reorder_point": int(rng.integers(50, 400)),
        })
    return pd.DataFrame(rows)


def build_warehouses() -> pd.DataFrame:
    data = [
        ("WH01", "Dallas Distribution Center", "Dallas", "Texas", "United States", 120000, "Regional DC"),
        ("WH02", "Atlanta Fulfilment Hub", "Atlanta", "Georgia", "United States", 90000, "Fulfilment"),
        ("WH03", "Chicago Central DC", "Chicago", "Illinois", "United States", 150000, "Central DC"),
        ("WH04", "Rotterdam EU Hub", "Rotterdam", "South Holland", "Netherlands", 110000, "Regional DC"),
        ("WH05", "Frankfurt Cross-Dock", "Frankfurt", "Hesse", "Germany", 60000, "Cross-Dock"),
        ("WH06", "Bengaluru South Asia DC", "Bengaluru", "Karnataka", "India", 95000, "Regional DC"),
        ("WH07", "Singapore APAC Hub", "Singapore", "Singapore", "Singapore", 85000, "Regional DC"),
        ("WH08", "Monterrey LATAM DC", "Monterrey", "Nuevo Leon", "Mexico", 70000, "Fulfilment"),
    ]
    return pd.DataFrame(data, columns=["warehouse_id", "warehouse_name", "city", "state_region",
                                       "country", "capacity_units", "warehouse_type"])


def build_customers(rng: np.random.Generator, n: int = 600) -> pd.DataFrame:
    regions = {"North America": "United States", "Europe": "Germany", "APAC": "India", "LATAM": "Mexico"}
    rows = []
    for i in range(1, n + 1):
        region = rng.choice(list(regions), p=[0.4, 0.25, 0.25, 0.1])
        rows.append({
            "customer_id": f"CUS{i:05d}",
            "customer_name": f"{rng.choice(['Blue','Red','Green','Swift','Bright','Core','Metro','Urban'])} "
                             f"{rng.choice(['Retail','Wholesale','Distribution','Stores','Mart','Group'])} {i}",
            "segment": rng.choice(["Enterprise", "Mid-Market", "SMB"], p=[0.15, 0.35, 0.5]),
            "region": region,
            "country": regions[region],
            "signup_date": (START - pd.Timedelta(days=int(rng.integers(30, 1800)))).date().isoformat(),
        })
    return pd.DataFrame(rows)


def build_purchase_orders(rng, products, warehouses, suppliers, n: int = 6000) -> pd.DataFrame:
    sup = suppliers.set_index("supplier_id")
    prod = products.set_index("product_id")
    pids = rng.choice(products["product_id"], n)
    dates = START + pd.to_timedelta(rng.integers(0, (END - START).days - 60, n), unit="D")
    rows = []
    for k, (pid, d) in enumerate(zip(pids, dates), start=1):
        sid = prod.loc[pid, "supplier_id"]
        lt = int(sup.loc[sid, "avg_lead_time_days"])
        rel = float(sup.loc[sid, "reliability_score"])
        expected = d + pd.Timedelta(days=lt)
        late = rng.random() > rel
        delay = int(rng.integers(1, 15)) if late else int(rng.integers(-2, 2))
        actual = expected + pd.Timedelta(days=delay)
        qty = int(rng.integers(100, 2000))
        short = rng.random() < 0.08
        received = int(qty * rng.uniform(0.8, 0.99)) if short else qty
        status = "Received" if actual <= END else "In Transit"
        if status == "In Transit":
            actual, received = pd.NaT, 0
        elif rng.random() < 0.01:
            status, received = "Cancelled", 0
            actual = pd.NaT
        rows.append({
            "po_id": f"PO{k:07d}",
            "po_date": d.date().isoformat(),
            "supplier_id": sid,
            "product_id": pid,
            "warehouse_id": rng.choice(warehouses["warehouse_id"]),
            "quantity_ordered": qty,
            "unit_cost": round(float(prod.loc[pid, "unit_cost"]) * float(rng.uniform(0.95, 1.08)), 2),
            "expected_delivery_date": expected.date().isoformat(),
            "actual_delivery_date": None if pd.isna(actual) else actual.date().isoformat(),
            "quantity_received": received,
            "po_status": status,
        })
    return pd.DataFrame(rows)


def build_sales_orders(rng, customers, products, warehouses, n: int = 20000) -> pd.DataFrame:
    prod = products.set_index("product_id")
    pids = rng.choice(products["product_id"], n)
    # Seasonality: heavier volume in Q4
    days = pd.date_range(START, END)
    w = np.array([1.35 if d.month in (10, 11, 12) else 1.0 for d in days])
    dates = rng.choice(days, n, p=w / w.sum())
    rows = []
    for k, (pid, d) in enumerate(zip(pids, dates), start=1):
        price = float(prod.loc[pid, "unit_price"])
        rows.append({
            "order_id": f"SO{k:08d}",
            "order_date": pd.Timestamp(d).date().isoformat(),
            "customer_id": rng.choice(customers["customer_id"]),
            "product_id": pid,
            "warehouse_id": rng.choice(warehouses["warehouse_id"]),
            "quantity": int(rng.integers(1, 120)),
            "unit_price": price,
            "discount_pct": float(rng.choice([0, 0, 0, 0.05, 0.1, 0.15])),
            "order_status": rng.choice(["Delivered", "Shipped", "Cancelled", "Returned"],
                                       p=[0.86, 0.05, 0.05, 0.04]),
        })
    return pd.DataFrame(rows)


def build_shipments(rng, sales: pd.DataFrame) -> pd.DataFrame:
    shippable = sales[sales["order_status"].isin(["Delivered", "Shipped", "Returned"])].copy()
    carriers = {"DHL": 0.92, "FedEx": 0.9, "UPS": 0.91, "Maersk": 0.82, "BlueDart": 0.85}
    modes = {"Air": (2, 7.5), "Road": (3, 2.0), "Sea": (18, 0.6), "Rail": (7, 1.0)}
    rows = []
    for k, r in enumerate(shippable.itertuples(index=False), start=1):
        carrier = rng.choice(list(carriers))
        mode = rng.choice(list(modes), p=[0.2, 0.55, 0.15, 0.1])
        ship = pd.Timestamp(r.order_date) + pd.Timedelta(days=int(rng.integers(0, 4)))
        transit = modes[mode][0] + int(rng.integers(0, 3))
        expected = ship + pd.Timedelta(days=transit)
        late = rng.random() > carriers[carrier]
        actual = expected + pd.Timedelta(days=int(rng.integers(1, 8)) if late else int(rng.integers(-1, 1)))
        delivered = r.order_status != "Shipped" and actual <= END
        rows.append({
            "shipment_id": f"SHP{k:08d}",
            "order_id": r.order_id,
            "warehouse_id": r.warehouse_id,
            "carrier": carrier,
            "shipping_mode": mode,
            "ship_date": ship.date().isoformat(),
            "expected_delivery_date": expected.date().isoformat(),
            "actual_delivery_date": actual.date().isoformat() if delivered else None,
            "shipping_cost": round(float(r.quantity) * modes[mode][1] * float(rng.uniform(0.8, 1.3)), 2),
            "shipment_status": "Delivered" if delivered else "In Transit",
        })
    return pd.DataFrame(rows)


def build_inventory(rng, products, warehouses, sales, pos) -> pd.DataFrame:
    snapshots = pd.date_range(START, END, freq="W-SUN")
    rows = []
    # simple per-(wh, product) stock walk driven by random demand/replenishment
    for wh in warehouses["warehouse_id"]:
        for pid, rop in zip(products["product_id"], products["reorder_point"]):
            qty = int(rop * rng.uniform(1.5, 4))
            for snap in snapshots:
                demand = int(rng.poisson(rop * 0.12))
                qty = max(0, qty - demand)
                on_order = 0
                if qty < rop:
                    on_order = int(rop * rng.uniform(1.0, 2.5))
                    if rng.random() < 0.55:  # replenishment lands
                        qty += on_order
                        on_order = 0
                rows.append({
                    "snapshot_date": snap.date().isoformat(),
                    "warehouse_id": wh,
                    "product_id": pid,
                    "quantity_on_hand": qty,
                    "quantity_reserved": int(qty * rng.uniform(0, 0.25)),
                    "quantity_on_order": on_order,
                })
    return pd.DataFrame(rows)


def inject_dirty_data(rng, sales, shipments, pos):
    """Add realistic quality issues. Documented in docs/data_dictionary.md."""
    # 1) Exact duplicate sales rows (~0.8%)
    dupes = sales.sample(frac=0.008, random_state=1)
    sales = pd.concat([sales, dupes], ignore_index=True).sample(frac=1, random_state=2).reset_index(drop=True)
    # 2) Null discount values (~1.5%)
    idx = sales.sample(frac=0.015, random_state=3).index
    sales["discount_pct"] = sales["discount_pct"].astype(object)
    sales.loc[idx, "discount_pct"] = None
    # 3) Negative / zero quantities (~0.3%)
    idx = sales.sample(frac=0.003, random_state=4).index
    sales.loc[idx, "quantity"] = -sales.loc[idx, "quantity"]
    # 4) Inconsistent status casing / whitespace in shipments (~3%)
    idx = shipments.sample(frac=0.03, random_state=5).index
    shipments.loc[idx, "shipment_status"] = shipments.loc[idx, "shipment_status"].str.upper() + " "
    idx = shipments.sample(frac=0.02, random_state=6).index
    shipments.loc[idx, "carrier"] = shipments.loc[idx, "carrier"].str.lower()
    # 5) Shipments dated before the order (~0.2%) -> logical date error
    idx = shipments.sample(frac=0.002, random_state=7).index
    shipments.loc[idx, "ship_date"] = "2023-12-25"
    # 6) Orphan foreign key in sales (product that doesn't exist) (~0.1%)
    idx = sales.sample(frac=0.001, random_state=8).index
    sales.loc[idx, "product_id"] = "PRD99999"
    # 7) PO received qty greater than ordered (~0.3%)
    idx = pos[pos["po_status"] == "Received"].sample(frac=0.003, random_state=9).index
    pos.loc[idx, "quantity_received"] = pos.loc[idx, "quantity_ordered"] + 500
    return sales, shipments, pos


def main(out: Path, seed: int) -> None:
    rng = np.random.default_rng(seed)
    out.mkdir(parents=True, exist_ok=True)

    suppliers = build_suppliers(rng)
    products = build_products(rng, suppliers)
    warehouses = build_warehouses()
    customers = build_customers(rng)
    pos = build_purchase_orders(rng, products, warehouses, suppliers)
    sales = build_sales_orders(rng, customers, products, warehouses)
    shipments = build_shipments(rng, sales)
    inventory = build_inventory(rng, products, warehouses, sales, pos)
    sales, shipments, pos = inject_dirty_data(rng, sales, shipments, pos)

    tables = {
        "suppliers": suppliers, "products": products, "warehouses": warehouses,
        "customers": customers, "purchase_orders": pos, "sales_orders": sales,
        "shipments": shipments, "inventory_snapshots": inventory,
    }
    for name, df in tables.items():
        df.to_csv(out / f"{name}.csv", index=False)
        print(f"{name:<20} {len(df):>8,} rows  {len(df.columns):>2} cols")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=Path, default=Path("data/raw"))
    ap.add_argument("--seed", type=int, default=42)
    a = ap.parse_args()
    main(a.out, a.seed)
