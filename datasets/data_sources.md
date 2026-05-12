# Dataset Sources — Healthcare Supply Chain Analytics Platform

This document lists all datasets used, with verified public URLs, confirmed column
schemas, and explicit mapping to the project's internal schema. Two datasets are from
public sources; two are generated synthetically in the same style.

---

## Dataset 1 — USAID Supply Chain Shipment & Pricing Data  *(Primary shipment source)*

| Attribute | Detail |
|---|---|
| **URL** | https://data.usaid.gov/Global-Health-Supply-Chain-Procurement-Database |
| **Kaggle Mirror** | https://www.kaggle.com/datasets/apoorvwatsky/supply-chain-shipment-pricing-data |
| **License** | CC-BY (USAID Open Data) |
| **Records** | ~10,000 rows |
| **File** | `SCMS_Delivery_History_Dataset.csv` |

### Confirmed Source Columns

```
id, project_code, pq_#, po_so_#, asn_dn_#, country, managed_by,
fulfill_via, vendor_inco_term, shipment_mode,
pq_first_sent_to_client_date, po_sent_to_vendor_date,
scheduled_delivery_date, delivered_to_client_date, delivery_recorded_date,
product_group, sub_classification, vendor, item_description,
molecule_test_type, brand, dosage, dosage_form, unit_of_measure_per_pack,
line_item_quantity, line_item_value, pack_price, unit_price,
manufacturing_site, first_line_designation,
weight_kilograms, freight_cost_usd, line_item_insurance_usd
```

### Mapping to Project Schema (`fact_shipments`)

| Source Column | Project Column | Notes |
|---|---|---|
| `id` | `shipment_id` | Direct use |
| `vendor` | `supplier_name` | Used to derive `supplier_id` |
| `country` | `hospital_id` (destination) | Country treated as hospital proxy |
| `po_sent_to_vendor_date` | `ship_date` | Date PO sent = start of shipment |
| `scheduled_delivery_date` | `expected_delivery_date` | — |
| `delivered_to_client_date` | `actual_delivery_date` | — |
| `shipment_mode` | `carrier` | Air / Sea / Truck |
| `line_item_quantity` | `quantity_shipped` | — |
| `line_item_value` | `shipment_cost` | USD total value |
| `unit_price` | `cost_per_unit` | — |
| `weight_kilograms` | `weight_kg` | — |
| `freight_cost_usd` | `freight_cost` | — |
| `product_group` | `category` | ARV / HRDT / etc. |
| `item_description` | `product_name` | — |
| `first_line_designation` | `is_critical` | TRUE/FALSE critical item |
| *(derived)* | `priority` | Mapped from `product_group`: ARV → CRITICAL, etc. |
| *(derived)* | `is_sla_met` | `delivered_to_client_date` ≤ `scheduled_delivery_date` |
| *(derived)* | `delivery_delay_days` | `delivered_to_client_date` − `scheduled_delivery_date` |

---

## Dataset 2 — Hospital Supply Chain  *(Inventory source)*

| Attribute | Detail |
|---|---|
| **URL** | https://www.kaggle.com/datasets/vanpatangan/hospital-supply-chain |
| **Posted** | October 2024 |
| **License** | CC0 Public Domain |
| **File** | Available via Kaggle API download |

This dataset covers hospital supply management including stock levels, reorder points,
and supplier assignments. It is used as the basis for `fact_inventory`.

> **Note:** Because Kaggle requires authentication for direct download, the
> `datasets/generate_inventory_sample.py` script produces a synthetic dataset
> with the same column structure for local development and testing.

### Column Mapping to Project Schema (`fact_inventory`)

| Source Column | Project Column |
|---|---|
| `item_id` | `product_id` |
| `item_name` | `product_name` |
| `category` | `category` |
| `hospital_id` | `hospital_id` |
| `supplier_id` | `supplier_id` |
| `quantity_on_hand` | `quantity_on_hand` |
| `reorder_level` | `reorder_level` |
| `safety_stock` | `safety_stock` |
| `unit_cost` | `unit_cost` |
| `last_restocked_date` | `last_restocked_dt` |
| `record_date` | `record_date` |

---

## Dataset 3 — DataCo Smart Supply Chain  *(Procurement & orders supplement)*

| Attribute | Detail |
|---|---|
| **URL** | https://www.kaggle.com/datasets/shashwatwork/dataco-smart-supply-chain-for-big-data-analysis |
| **Source** | Mendeley Data / DataCo Global |
| **License** | CC BY 4.0 |
| **Records** | ~180,000 rows |
| **File** | `DataCoSupplyChainDataset.csv` |

### Confirmed Source Columns (selected)

```
Order Id, Order Date (DateOrders), Shipping Date (DateOrders),
Order Status, Order Item Quantity, Order Item Total, Sales,
Order Profit Per Order, Shipping Mode,
Days for shipping (real), Days for shipment (scheduled),
Delivery Status, Late_delivery_risk,
Category Name, Department Name, Product Name, Product Price,
Customer Id, Customer Country, Customer State, Customer City
```

### Mapping to Project Schema (`fact_procurement`)

| Source Column | Project Column | Notes |
|---|---|---|
| `Order Id` | `purchase_order_id` | — |
| `Order Date (DateOrders)` | `order_date` | — |
| `Shipping Date (DateOrders)` | `ship_date` | — |
| `Order Item Quantity` | `quantity_ordered` | — |
| `Order Item Total` | `total_order_value` | — |
| `Product Name` | `product_name` | — |
| `Category Name` | `category` | — |
| `Shipping Mode` | `carrier` | Standard Class → STANDARD, etc. |
| `Days for shipment (scheduled)` | `lead_time_days` | — |
| `Order Status` | `po_status` | COMPLETE → RECEIVED, etc. |
| `Order Profit Per Order` | *(derived)* `cost_variance` | Treat as budget delta |
| `Late_delivery_risk` | `is_sla_met` | Inverted: 0 → TRUE |

---

## Dataset 4 — Supplier Master  *(Synthetic)*

Generated synthetically in the style of real vendor master data. No external source
required; produced by `datasets/generate_suppliers.py`.

### Schema

```
supplier_id, supplier_name, supplier_type, country, state, city,
contact_email, contract_start_dt, contract_end_dt, payment_terms,
preferred_flag, reliability_tier, reliability_score, is_active
```

---

## Entity Relationships

```
dim_hospital (hospital_id)
       │
       ├──► fact_inventory ◄── dim_product (product_id)
       │              └──── dim_supplier (supplier_id)
       │
       ├──► fact_shipments ◄── dim_supplier
       │              └──── dim_product
       │
       └──► fact_procurement ◄── dim_supplier
                         └──── dim_product

All fact tables join to dim_date via date_key.
```

---

## Download Instructions

```bash
# USAID dataset (direct — no auth required)
curl -L "https://data.usaid.gov/api/views/a3rc-nmf6/rows.csv?accessType=DOWNLOAD" \
  -o datasets/raw/SCMS_Delivery_History_Dataset.csv

# Kaggle datasets (requires Kaggle API key)
pip install kaggle
kaggle datasets download -d apoorvwatsky/supply-chain-shipment-pricing-data -p datasets/raw/
kaggle datasets download -d vanpatangan/hospital-supply-chain -p datasets/raw/
kaggle datasets download -d shashwatwork/dataco-smart-supply-chain-for-big-data-analysis -p datasets/raw/

# Generate synthetic supplier master
python datasets/generate_suppliers.py --output datasets/raw/suppliers.csv --count 500
```
