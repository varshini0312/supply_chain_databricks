# Supply Chain Lakehouse - Databricks Data Engineering Project

An end-to-end **medallion architecture** (Bronze -> Silver -> Gold) built on Databricks, Delta Lake and
Unity Catalog, using a realistic synthetic supply chain dataset (suppliers, products, purchase orders,
sales orders, shipments, inventory).

> **Business problem:** Operations leaders can't see supplier reliability, shipment performance or stock risk
> in one place. This pipeline turns raw ERP / WMS / TMS extracts into trusted, analytics-ready KPIs.

## Architecture

```
 data/raw/*.csv ──▶  BRONZE  ──▶  SILVER  ──▶  GOLD  ──▶ BI / SQL dashboards
 (Unity Catalog     raw strings   typed, cleansed,   KPI marts &
  Volume)           + lineage     deduped, DQ-gated   fact tables
                                  + quarantine
                                        │
                                        └──▶ ops.dq_results (audit of every check)
```

See `docs/architecture.md` for the diagram and design decisions.

## Repository layout

| Path | Purpose |
|---|---|
| `data/raw/` | 8 CSV source extracts (~146K rows, 6.8 MB) |
| `notebooks/00_setup_and_config.py` | Creates catalog, schemas, landing volume |
| `notebooks/01_bronze_ingestion.py` | Raw load + lineage columns (`_ingested_at`, `_source_file`, `_batch_id`) |
| `notebooks/02_silver_transformations.py` | Typing, cleansing, dedupe, rule validation, quarantine tables |
| `notebooks/03_gold_marts.py` | `fact_sales` + 4 KPI marts |
| `notebooks/04_data_quality_checks.py` | Assertions logged to `ops.dq_results`; fails job on errors |
| `databricks.yml`, `resources/` | Databricks Asset Bundle: 5-task workflow with dependencies & schedule |
| `src/generate_data.py` | Reproducible data generator (`--seed 42`) |
| `tests/` | Local pytest checks on raw data |
| `docs/` | Data dictionary, architecture, KPI definitions |

## Quick start (Databricks workspace with Unity Catalog)

1. **Import** the `notebooks/` folder into your workspace (Workspace -> Import, or connect this repo via Repos / Git folders).
2. **Run `00_setup_and_config`** - creates `supply_chain` catalog, `bronze/silver/gold/ops` schemas and the volume
   `/Volumes/supply_chain/bronze/raw_landing`.
3. **Upload the CSVs** from `data/raw/` into that volume (Catalog Explorer -> the volume -> *Upload to this volume*), or via CLI:
   ```bash
   databricks fs cp data/raw/ dbfs:/Volumes/supply_chain/bronze/raw_landing/ --recursive
   ```
4. **Run notebooks 01 -> 04** in order on any cluster / serverless compute (DBR 13.3+).
5. *(Optional)* Deploy as a scheduled workflow:
   ```bash
   databricks bundle validate
   databricks bundle deploy -t dev
   databricks bundle run supply_chain_medallion_pipeline -t dev
   ```

> No Unity Catalog (e.g. Community Edition)? Change the catalog widget to `hive_metastore`, upload CSVs to
> DBFS (`/FileStore/...`) and edit the `landing` path in notebook 01.

## Gold layer outputs

| Table | Grain | Key metrics |
|---|---|---|
| `gold.fact_sales` | order line | net revenue, COGS, gross margin %, partitioned by year |
| `gold.mart_supplier_performance` | supplier x month | on-time, in-full, **OTIF**, fill rate, lead time, 3-month rolling OTIF |
| `gold.mart_shipment_performance` | carrier x mode x month | on-time %, avg delay, cost per unit |
| `gold.mart_inventory_health` | warehouse x product (latest week) | weeks of cover, stock-out / below-ROP / overstock status |
| `gold.mart_warehouse_kpis` | warehouse x month | orders, units, revenue, margin, throughput vs capacity |

### Example analytics queries
```sql
-- Worst 10 suppliers by OTIF over the last 6 months of data
SELECT supplier_name, country, tier, ROUND(AVG(otif_rate),3) AS otif, SUM(total_spend) AS spend
FROM supply_chain.gold.mart_supplier_performance
WHERE month >= add_months((SELECT MAX(month) FROM supply_chain.gold.mart_supplier_performance), -6)
GROUP BY supplier_name, country, tier ORDER BY otif ASC LIMIT 10;

-- Products at stock-out risk
SELECT warehouse_name, sku, quantity_available, reorder_point, weeks_of_cover
FROM supply_chain.gold.mart_inventory_health
WHERE stock_status IN ('Stock-out','Below Reorder Point') ORDER BY weeks_of_cover;

-- Carrier scorecard
SELECT carrier, shipping_mode, SUM(shipments) shipments, ROUND(AVG(on_time_rate),3) on_time, ROUND(AVG(cost_per_unit),2) cpu
FROM supply_chain.gold.mart_shipment_performance GROUP BY carrier, shipping_mode ORDER BY on_time DESC;
```

## Engineering practices demonstrated
- Medallion architecture with Delta Lake and Unity Catalog three-level namespace
- Lineage metadata on every Bronze row; schema-on-read as strings to keep ingestion resilient
- **Quarantine pattern** - invalid rows are retained with failure reasons, never silently dropped
- Idempotent, parameterised notebooks (widgets) - promotable across dev / prod catalogs
- Data-quality gate (row reconciliation, PK uniqueness, nulls, referential integrity, domain rules, gold-vs-silver revenue tie-out)
- Window functions (dedupe, rolling averages), partitioned fact table, table comments for discoverability
- Infrastructure as code with Databricks Asset Bundles; reproducible data generation; unit tests

## Regenerating the data
```bash
pip install -r requirements.txt
python src/generate_data.py --out data/raw --seed 42
pytest -q
```

## Possible extensions
Auto Loader streaming ingestion · Delta Live Tables / Lakeflow declarative pipelines · SCD Type 2 supplier dimension ·
`MERGE`-based incremental Silver · demand forecasting with MLflow · Databricks SQL dashboard on the gold marts.
