# Data Dictionary (raw layer)

All files are UTF-8 CSV with headers. Dates are ISO `YYYY-MM-DD`. Coverage: **2024-01-01 to 2025-12-31**.

| File | Rows | Grain | Primary key |
|---|---:|---|---|
| suppliers.csv | 60 | one row per supplier | supplier_id |
| products.csv | 120 | one row per SKU | product_id |
| warehouses.csv | 8 | one row per warehouse | warehouse_id |
| customers.csv | 600 | one row per customer | customer_id |
| purchase_orders.csv | 6,000 | one PO line | po_id |
| sales_orders.csv | 20,160 | one sales order line (incl. duplicates) | order_id |
| shipments.csv | 18,964 | one outbound shipment per shipped order | shipment_id |
| inventory_snapshots.csv | 99,840 | weekly (Sunday) stock per warehouse x product | snapshot_date + warehouse_id + product_id |

## Key columns
**suppliers**: supplier_id, supplier_name, country, city, tier (Strategic/Preferred/Approved), avg_lead_time_days, reliability_score (0-1), payment_terms, onboarded_date, is_active

**products**: product_id, sku, product_name, category, subcategory, unit_cost, unit_price, weight_kg, supplier_id (FK), shelf_life_days (0 = non-perishable), reorder_point

**warehouses**: warehouse_id, warehouse_name, city, state_region, country, capacity_units, warehouse_type

**customers**: customer_id, customer_name, segment (Enterprise/Mid-Market/SMB), region, country, signup_date

**purchase_orders**: po_id, po_date, supplier_id (FK), product_id (FK), warehouse_id (FK), quantity_ordered, unit_cost, expected_delivery_date, actual_delivery_date (null if not received), quantity_received, po_status (Received / In Transit / Cancelled)

**sales_orders**: order_id, order_date, customer_id (FK), product_id (FK), warehouse_id (FK), quantity, unit_price, discount_pct (0-1), order_status (Delivered / Shipped / Cancelled / Returned)

**shipments**: shipment_id, order_id (FK), warehouse_id, carrier, shipping_mode (Air/Road/Sea/Rail), ship_date, expected_delivery_date, actual_delivery_date (null if in transit), shipping_cost, shipment_status

**inventory_snapshots**: snapshot_date, warehouse_id (FK), product_id (FK), quantity_on_hand, quantity_reserved, quantity_on_order

## Intentionally injected data-quality issues
These exist so the Silver layer has real cleansing work. Each is handled in `02_silver_transformations.py` and verified in `04_data_quality_checks.py`.

| # | Table | Issue | Approx. share | Silver handling |
|---|---|---|---:|---|
| 1 | sales_orders | Exact duplicate rows | 0.8% | Dedupe on `order_id` |
| 2 | sales_orders | Null `discount_pct` | 1.5% | Default to 0 |
| 3 | sales_orders | Negative `quantity` | 0.3% | Quarantine |
| 4 | sales_orders | `product_id` not in products (PRD99999) | 0.1% | Quarantine |
| 5 | shipments | Status in UPPERCASE with trailing space | 3% | Trim + initcap |
| 6 | shipments | Carrier in lowercase | 2% | Map to canonical name |
| 7 | shipments | `ship_date` earlier than order date | 0.2% | Quarantine |
| 8 | purchase_orders | `quantity_received` > `quantity_ordered` | 0.3% of received | Quarantine |

## KPI definitions
- **On-time (PO)**: `actual_delivery_date <= expected_delivery_date`
- **In-full**: `quantity_received >= quantity_ordered`
- **OTIF**: on-time AND in-full
- **Fill rate**: `quantity_received / quantity_ordered`
- **Weeks of cover**: `quantity_available / avg weekly demand (trailing 12 weeks)`
- **Net revenue**: `quantity x unit_price x (1 - discount_pct)`
