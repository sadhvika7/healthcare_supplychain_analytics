# Data Dictionary

## fact_inventory
*Source: Hospital Supply Chain (Kaggle/vanpatangan)*

| Column | Type | Source Column | Description |
|---|---|---|---|
| inventory_fact_key | BIGINT | — | Surrogate PK |
| date_key | INTEGER | record_date | FK → dim_date |
| hospital_key | INTEGER | hospital_id | FK → dim_hospital |
| product_key | INTEGER | item_id | FK → dim_product |
| supplier_key | INTEGER | supplier_id | FK → dim_supplier |
| inventory_id | VARCHAR(100) | item_id+hospital_id+record_date | Natural composite key |
| quantity_on_hand | INTEGER | quantity_on_hand | Current units in stock |
| reorder_level | INTEGER | reorder_level | Reorder trigger threshold |
| safety_stock | INTEGER | safety_stock | Minimum buffer required |
| unit_cost | DECIMAL(10,4) | unit_cost | Cost per unit USD |
| inventory_value | DECIMAL(14,2) | derived | quantity × unit_cost |
| stock_status | VARCHAR(20) | derived | STOCKOUT/CRITICAL_LOW/LOW/ADEQUATE/OVERSTOCK |
| is_critical_shortage | BOOLEAN | derived | True if STOCKOUT or CRITICAL_LOW |
| restock_urgency_score | SMALLINT | derived | 0–100 urgency score |

## fact_shipments
*Source: USAID SCMS Delivery History Dataset*

| Column | Type | USAID Source Column | Description |
|---|---|---|---|
| shipment_fact_key | BIGINT | — | Surrogate PK |
| ship_date_key | INTEGER | po_sent_to_vendor_date | FK → dim_date |
| delivery_date_key | INTEGER | delivered_to_client_date | FK → dim_date |
| supplier_key | INTEGER | vendor | FK → dim_supplier |
| shipment_id | VARCHAR(50) | id | Natural key from USAID |
| quantity_shipped | INTEGER | line_item_quantity | Units in shipment |
| shipment_cost | DECIMAL(12,2) | line_item_value | Total USD value |
| cost_per_unit | DECIMAL(10,4) | unit_price | USD per unit |
| freight_cost | DECIMAL(12,2) | freight_cost_usd | Freight charge USD |
| priority | VARCHAR(20) | product_group (mapped) | CRITICAL/HIGH/STANDARD/LOW |
| carrier | VARCHAR(100) | shipment_mode | Air/Sea/Truck |
| is_sla_met | BOOLEAN | derived | Delivered ≤ scheduled date |
| is_critical | BOOLEAN | first_line_designation | First-line ARV/test flag |
| delivery_delay_days | SMALLINT | derived | Negative = early |
| delivery_performance | VARCHAR(30) | derived | EARLY/ON_TIME/SLIGHTLY_LATE/SIGNIFICANTLY_LATE |

## fact_procurement
*Source: DataCo Smart Supply Chain*

| Column | Type | DataCo Source Column | Description |
|---|---|---|---|
| procurement_fact_key | BIGINT | — | Surrogate PK |
| purchase_order_id | VARCHAR(50) | 'PO-' + Order_Id | Natural key |
| quantity_ordered | INTEGER | Order_Item_Quantity | Units ordered |
| unit_price | DECIMAL(10,4) | Product_Price | USD per unit |
| total_order_value | DECIMAL(14,2) | Order_Item_Total | Total USD |
| budget_amount | DECIMAL(14,2) | derived | total × 1.05 |
| cost_variance_pct | DECIMAL(6,2) | derived | (actual−budget)/budget×100 |
| lead_time_days | SMALLINT | Days_for_shipment_scheduled | Planned lead time |
| po_status | VARCHAR(30) | Order_Status (mapped) | DRAFT/APPROVED/RECEIVED/CANCELLED |
| is_sla_met | BOOLEAN | Late_delivery_risk (inverted) | 0 → TRUE |

## dim_supplier *(SCD Type 2)*

| Column | Type | Description |
|---|---|---|
| supplier_key | INTEGER | Surrogate PK |
| supplier_id | VARCHAR(50) | SUP-{6-char hash of vendor name} |
| supplier_name | VARCHAR(200) | USAID: vendor column |
| reliability_tier | VARCHAR(20) | PLATINUM/GOLD/SILVER/WATCH_LIST |
| reliability_score | NUMERIC(5,2) | 0–100 composite score |
| is_current | BOOLEAN | SCD Type 2 current record flag |
| effective_start_dt | DATE | Record version start |
| effective_end_dt | DATE | 9999-12-31 = current |
