-- dbt Model: stg_inventory
-- Layer: Staging
-- Source: Hospital Supply Chain dataset (Kaggle)
-- URL: https://www.kaggle.com/datasets/vanpatangan/hospital-supply-chain
--
-- Source columns (post-Glue rename):
--   item_id               → product_id  (standardised)
--   item_name             → product_name
--   category, hospital_id, supplier_id
--   quantity_on_hand, reorder_level, safety_stock, unit_cost
--   last_restocked_date   → last_restocked_dt
--   record_date
-- Derived in Glue:
--   inventory_id          = item_id + hospital_id + record_date (composite)
--   stock_status, is_critical_shortage, restock_urgency_score, inventory_value

{{
  config(
    materialized = 'view',
    schema       = 'staging',
    tags         = ['staging', 'inventory', 'daily']
  )
}}

WITH source AS (
    SELECT * FROM {{ source('hsc_raw', 'inventory_raw') }}
),

cleaned AS (
    SELECT
        UPPER(TRIM(inventory_id))               AS inventory_id,
        UPPER(TRIM(product_id))                 AS product_id,
        UPPER(TRIM(hospital_id))                AS hospital_id,
        UPPER(TRIM(supplier_id))                AS supplier_id,
        UPPER(TRIM(product_name))               AS product_name,
        UPPER(TRIM(category))                   AS category,
        CAST(quantity_on_hand  AS INTEGER)      AS quantity_on_hand,
        CAST(reorder_level     AS INTEGER)      AS reorder_level,
        CAST(safety_stock      AS INTEGER)      AS safety_stock,
        CAST(unit_cost         AS DECIMAL(10,4))AS unit_cost,
        CAST(inventory_value   AS DECIMAL(14,2))AS inventory_value,
        CAST(last_restocked_dt AS DATE)         AS last_restocked_dt,
        CAST(record_date       AS DATE)         AS record_date,
        -- Business rule columns (derived in Glue job)
        UPPER(TRIM(stock_status))               AS stock_status,
        CAST(is_critical_shortage  AS BOOLEAN)  AS is_critical_shortage,
        CAST(restock_urgency_score AS INTEGER)  AS restock_urgency_score,
        CAST(days_since_restock    AS INTEGER)  AS days_since_restock,
        -- Audit
        etl_run_date,
        etl_load_ts

    FROM source
    WHERE inventory_id IS NOT NULL
      AND product_id    IS NOT NULL
      AND hospital_id   IS NOT NULL
      AND record_date   IS NOT NULL
      AND quantity_on_hand >= 0
)

SELECT * FROM cleaned
