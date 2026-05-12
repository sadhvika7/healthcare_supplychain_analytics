# 🏥 Intelligent Healthcare Supply Chain Analytics Platform

> An enterprise-grade end-to-end data engineering platform for monitoring inventory, procurement, shipment performance, and supplier reliability across healthcare networks.

[![AWS](https://img.shields.io/badge/AWS-Glue%20%7C%20S3%20%7C%20Redshift-orange)](https://aws.amazon.com/)
[![PySpark](https://img.shields.io/badge/PySpark-3.5.1-red)](https://spark.apache.org/)
[![Airflow](https://img.shields.io/badge/Airflow-2.9.2-blue)](https://airflow.apache.org/)
[![dbt](https://img.shields.io/badge/dbt-1.8.4-orange)](https://getdbt.com/)
[![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-yellow)](https://powerbi.microsoft.com/)

---

## 📌 Business Problem

Healthcare organizations face recurring operational challenges that affect patient care and operational efficiency:

| Challenge | Operational Impact |
|---|---|
| Delayed medical supply deliveries | Disrupted patient care workflows |
| Inventory shortages of critical items | Operational halts and emergency procurement |
| Overstocking of low-demand supplies | Budget waste and storage burden |
| Poor supplier visibility | Reactive rather than proactive procurement |
| Manual, siloed reporting | Decision-making lag of 2–5 days |
| Inconsistent KPI definitions across departments | Misaligned operational goals |

This platform addresses each of these by building automated data pipelines, a centralized data warehouse, and operational dashboards — enabling data-driven decisions at both the operational and executive level.

---

## 🏗️ Architecture

```
Source Files (CSV)
       │
       ▼
┌──────────────────────────────────────────┐
│   AWS S3 — Raw Layer                     │
│   s3://hsc-analytics-raw/                │
│   ├── inventory/    (Kaggle HSC)         │
│   ├── shipments/    (USAID SCMS)         │
│   ├── procurement/  (DataCo)             │
│   └── suppliers/    (Synthetic)          │
└───────────────┬──────────────────────────┘
                │
                ▼
┌──────────────────────────────────────────┐
│   AWS Glue Crawlers                      │
│   AWS Glue Data Catalog                  │
└───────────────┬──────────────────────────┘
                │
                ▼
┌──────────────────────────────────────────┐
│   AWS Glue ETL Jobs (PySpark 3.5.1)      │
│   ├── inventory_processing.py            │
│   ├── shipment_processing.py             │
│   ├── procurement_processing.py          │
│   └── kpi_aggregation.py                │
└───────────────┬──────────────────────────┘
                │
                ▼
┌──────────────────────────────────────────┐
│   AWS S3 — Curated Layer                 │
│   Parquet format, partitioned by date    │
└───────────────┬──────────────────────────┘
                │
                ▼
┌──────────────────────────────────────────┐
│   Amazon Redshift (Star Schema)          │
│   fact_inventory / fact_shipments        │
│   fact_procurement                       │
│   dim_supplier / dim_hospital            │
│   dim_product  / dim_date                │
└───────────────┬──────────────────────────┘
                │
                ▼
┌──────────────────────────────────────────┐
│   dbt 1.8.4                              │
│   Staging → Marts → KPI Models           │
│   + Automated schema tests               │
└───────────────┬──────────────────────────┘
                │
                ▼
┌──────────────────────────────────────────┐
│   Power BI Dashboards                    │
│   Executive / Inventory / Shipment /     │
│   Supplier Performance                   │
└──────────────────────────────────────────┘

Orchestration : Apache Airflow 2.9.2
Monitoring    : AWS CloudWatch + Airflow Logs + Audit Tables
Quality       : PySpark QC Framework + dbt schema tests
```

---

## 📁 Project Structure

```
healthcare-supplychain-analytics/
│
├── airflow/
│   └── dags/
│       └── supply_chain_pipeline_dag.py   # Full orchestration DAG
│
├── glue_jobs/
│   ├── inventory_processing.py            # Kaggle HSC → curated inventory
│   ├── shipment_processing.py             # USAID SCMS → curated shipments
│   ├── procurement_processing.py          # DataCo → curated procurement
│   ├── kpi_aggregation.py                 # Cross-domain KPI computation
│   └── utils/
│       └── constants.py                   # S3 paths, SLA thresholds, KPI targets
│
├── dbt/
│   ├── dbt_project.yml
│   ├── packages.yml                       # dbt_utils dependency
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_inventory.sql          # Hospital Supply Chain columns
│   │   │   ├── stg_shipments.sql          # USAID SCMS columns
│   │   │   ├── stg_procurement.sql        # DataCo columns
│   │   │   └── stg_suppliers.sql
│   │   ├── marts/
│   │   │   ├── fact_inventory.sql         # Incremental, DISTKEY=hospital_key
│   │   │   ├── fact_shipments.sql         # Incremental, DISTKEY=supplier_key
│   │   │   ├── dim_supplier.sql           # SCD Type 2 via snapshot
│   │   │   └── dim_date.sql               # 2006-01-01 to 2030-12-31
│   │   └── kpi/
│   │       └── kpi_operational_summary.sql # Unified dashboard KPI table
│   ├── tests/
│   │   └── schema.yml                     # All quality tests (not_null, unique, etc.)
│   └── snapshots/
│       └── supplier_snapshot.sql          # SCD Type 2 on reliability_tier
│
├── sql/
│   └── redshift_schema.sql               # Full DDL with dataset source comments
│
├── data_quality/
│   └── quality_framework.py              # Reusable PySpark QC engine
│
├── dashboards/
│   └── dashboard_design_guide.md         # Power BI layout, DAX, RLS spec
│
├── datasets/
│   └── data_sources.md                   # Verified URLs, schemas, column mappings
│
├── docs/
│   ├── data_dictionary.md
│   ├── kpi_definitions.md
│   └── deployment_guide.md
│
├── requirements.txt                      # Versions verified available May 2025
└── README.md
```

---

## 📊 Datasets & Sources

| Dataset | Source | URL | Records |
|---|---|---|---|
| Shipments & Pricing | USAID SCMS (Kaggle mirror) | https://www.kaggle.com/datasets/apoorvwatsky/supply-chain-shipment-pricing-data | ~10K |
| Hospital Inventory | Kaggle (vanpatangan) | https://www.kaggle.com/datasets/vanpatangan/hospital-supply-chain | Varies |
| Procurement Orders | DataCo / Kaggle | https://www.kaggle.com/datasets/shashwatwork/dataco-smart-supply-chain-for-big-data-analysis | ~180K |
| Supplier Master | Synthetic | Generated via `datasets/generate_suppliers.py` | ~500 |

> See [`datasets/data_sources.md`](datasets/data_sources.md) for full column schemas, verified source URLs, and explicit column-to-project mappings for each dataset.

---

## ⚙️ Tech Stack

| Layer | Technology | Version |
|---|---|---|
| Cloud Storage | AWS S3 | — |
| ETL Engine | AWS Glue (PySpark) | Glue 4.0 / Spark 3.3 |
| Data Catalog | AWS Glue Data Catalog | — |
| Orchestration | Apache Airflow | 2.9.2 |
| Data Warehouse | Amazon Redshift | dc2.large |
| Transformations | dbt | 1.8.4 |
| Visualization | Power BI | — |
| Monitoring | AWS CloudWatch | — |
| Data Quality | PySpark QC Engine + dbt tests | — |

All Python library versions pinned in `requirements.txt` — verified available as of May 2025.

---

## 📈 Operational KPIs

| KPI | Formula | Target |
|---|---|---|
| Inventory Fill Rate | (Non-stockout SKUs / Total SKUs) × 100 | ≥ 95% |
| Shipment SLA % | (On-time deliveries / Total completed) × 100 | ≥ 90% |
| Delayed Shipment Count | Count where delivery_delay_days > 0 | Trending down |
| Avg Delivery Time | AVG(actual_transit_days) | ≤ 5 days |
| Critical Stock Shortage | Count where stock_status IN (STOCKOUT, CRITICAL_LOW) | 0 life-critical |
| Supplier Reliability Score | (on_time_rate × 0.70) + (max(0, 100 − avg_delay × 10) × 0.30) | ≥ 85 |
| Inventory Turnover Ratio | Total COGS / Avg Inventory Value | Category benchmark |
| Procurement Cost Variance | (Actual − Budget) / Budget × 100 | ≤ ±5% |
| Stockout % | (Stockout SKUs / Total SKUs) × 100 | ≤ 2% |
| Operational Health Score | (Fill Rate × 0.40) + (Shipment SLA × 0.60) | ≥ 88 |

---

## 🚀 Quick Start

### Prerequisites

```bash
python >= 3.9
AWS CLI v2 configured
Apache Airflow 2.9.2
dbt-redshift 1.8.4
```

### Step 1 — Create AWS Infrastructure

```bash
# S3 buckets
for bucket in hsc-analytics-raw hsc-analytics-curated hsc-analytics-scripts; do
  aws s3 mb s3://$bucket --region us-east-1
done

# IAM role for Glue (trust policy in docs/deployment_guide.md)
aws iam create-role --role-name GlueHSCRole \
  --assume-role-policy-document file://docs/glue-trust-policy.json
aws iam attach-role-policy --role-name GlueHSCRole \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole

# Redshift cluster
aws redshift create-cluster \
  --cluster-identifier hsc-analytics \
  --node-type dc2.large \
  --number-of-nodes 2 \
  --master-username admin \
  --master-user-password 'YourSecureP@ssword!' \
  --db-name hsc_dw
```

### Step 2 — Download & Upload Data

```bash
# All datasets via Kaggle API — requires ~/.kaggle/kaggle.json
# Get your token: https://www.kaggle.com/settings → API → Create New Token
# Note: data.usaid.gov is offline since 2025; SCMS dataset is preserved on Kaggle
pip install kaggle

kaggle datasets download -d apoorvwatsky/supply-chain-shipment-pricing-data -p ./raw/
kaggle datasets download -d vanpatangan/hospital-supply-chain               -p ./raw/
kaggle datasets download -d shashwatwork/dataco-smart-supply-chain-for-big-data-analysis -p ./raw/
unzip "./raw/*.zip" -d ./raw/
aws s3 sync ./raw/ s3://hsc-analytics-raw/
```

### Step 3 — Deploy Schema & Scripts

```bash
# Redshift schema
psql -h your-cluster.redshift.amazonaws.com -U admin -d hsc_dw \
  -f sql/redshift_schema.sql

# Glue scripts
aws s3 sync glue_jobs/ s3://hsc-analytics-scripts/glue_jobs/
```

### Step 4 — Configure Airflow

```bash
pip install -r requirements.txt
airflow db init

airflow connections add aws_default \
  --conn-type aws \
  --conn-extra '{"region_name":"us-east-1"}'

airflow connections add redshift_default \
  --conn-type redshift \
  --conn-host your-cluster.redshift.amazonaws.com \
  --conn-port 5439 --conn-schema hsc_dw

cp airflow/dags/*.py $AIRFLOW_HOME/dags/
airflow dags trigger supply_chain_pipeline
```

### Step 5 — Run dbt

```bash
cd dbt/
dbt deps           # install dbt_utils
dbt debug          # verify connection
dbt run            # build all models
dbt test           # run schema tests
dbt docs generate && dbt docs serve   # browse lineage
```

### Step 6 — Connect Power BI

1. Open Power BI Desktop → Get Data → Amazon Redshift
2. Server: `your-cluster.redshift.amazonaws.com:5439` | Database: `hsc_dw`
3. Import from schemas: `mart` and `kpi`
4. Follow [`dashboards/dashboard_design_guide.md`](dashboards/dashboard_design_guide.md) for layout and DAX

---

## 📚 Documentation

| Document | Description |
|---|---|
| [`datasets/data_sources.md`](datasets/data_sources.md) | Verified dataset URLs, exact column schemas, mapping to project |
| [`docs/kpi_definitions.md`](docs/kpi_definitions.md) | KPI formulas, targets, and ownership |
| [`docs/data_dictionary.md`](docs/data_dictionary.md) | Column-level definitions for all tables |
| [`docs/deployment_guide.md`](docs/deployment_guide.md) | Step-by-step AWS infrastructure setup |
| [`dashboards/dashboard_design_guide.md`](dashboards/dashboard_design_guide.md) | Power BI layout, DAX measures, RLS configuration |

---

## 🏛️ Data Model

```
dim_supplier ───────────────────────────────────────────┐
                                                        │
dim_hospital ──► fact_inventory ◄── dim_product ◄───────┤
                      │                                  │
dim_date ─────────────┤                                  │
                      └──► fact_shipments ◄──────────────┤
                                                        │
                           fact_procurement ◄────────────┘

All fact tables join to dim_date via date_key.
dim_supplier uses SCD Type 2 via dbt snapshot.
```

---

## 🔎 Data Quality

The `DataQualityEngine` in `data_quality/quality_framework.py` applies to every entity:

| Check Type | Example Rule | Severity |
|---|---|---|
| `NULL_CHECK` | `shipment_id` must not be null | ERROR |
| `DUPLICATE` | `inventory_id` must be unique per run | ERROR |
| `RANGE` | `quantity_on_hand >= 0` | ERROR |
| `ACCEPTED_VALUES` | `stock_status` in [STOCKOUT, CRITICAL_LOW, LOW, ADEQUATE, OVERSTOCK] | ERROR |
| `FRESHNESS` | `etl_load_ts` within 25 hours | WARN |

Failed records are quarantined to `s3://hsc-analytics-curated/quarantine/` with a reason tag. All results are logged to `s3://hsc-analytics-curated/audit/quality/`.
