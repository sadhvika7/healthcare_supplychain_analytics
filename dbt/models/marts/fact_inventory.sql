-- dbt Model: fact_inventory | Incremental Mart
-- Source: Hospital Supply Chain (Kaggle) via stg_inventory
{{
  config(materialized='incremental', unique_key='inventory_id',
    schema='mart', dist='hospital_key', sort=['date_key','product_key'],
    tags=['mart','fact','inventory'], on_schema_change='sync_all_columns')
}}
WITH inventory AS (
    SELECT * FROM {{ ref('stg_inventory') }}
    {% if is_incremental() %}
    WHERE record_date > (SELECT MAX(record_date) FROM {{ this }})
    {% endif %}
),
dim_date     AS (SELECT date_key, full_date FROM {{ ref('dim_date') }}),
dim_hospital AS (SELECT hospital_key, hospital_id FROM {{ ref('dim_hospital') }} WHERE is_active),
dim_product  AS (SELECT product_key, product_id FROM {{ ref('dim_product') }}),
dim_supplier AS (SELECT supplier_key, supplier_id FROM {{ ref('dim_supplier') }} WHERE is_current)
SELECT
    dd.date_key, dh.hospital_key, dp.product_key, ds.supplier_key,
    inv.inventory_id, inv.quantity_on_hand, inv.reorder_level, inv.safety_stock,
    inv.unit_cost, inv.inventory_value, inv.last_restocked_dt, inv.days_since_restock,
    inv.restock_urgency_score, inv.stock_status, inv.is_critical_shortage,
    inv.record_date, inv.etl_load_ts, CURRENT_TIMESTAMP AS dbt_updated_at
FROM inventory inv
LEFT JOIN dim_date     dd ON inv.record_date = dd.full_date
LEFT JOIN dim_hospital dh ON inv.hospital_id = dh.hospital_id
LEFT JOIN dim_product  dp ON inv.product_id  = dp.product_id
LEFT JOIN dim_supplier ds ON inv.supplier_id = ds.supplier_id
