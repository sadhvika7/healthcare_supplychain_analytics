-- dbt Model: stg_procurement
-- Source: DataCo Smart Supply Chain (Kaggle)
-- URL: https://www.kaggle.com/datasets/shashwatwork/dataco-smart-supply-chain-for-big-data-analysis
-- Mendeley: https://data.mendeley.com/datasets/8gx2fvg2k6/5

{{
  config(
    materialized = 'view',
    schema       = 'staging',
    tags         = ['staging', 'procurement', 'daily']
  )
}}

WITH source AS (
    SELECT * FROM {{ source('hsc_raw', 'procurement_raw') }}
)

SELECT
    UPPER(TRIM(purchase_order_id))            AS purchase_order_id,
    UPPER(TRIM(hospital_id))                  AS hospital_id,
    UPPER(TRIM(supplier_id))                  AS supplier_id,
    UPPER(TRIM(product_name))                 AS product_name,
    UPPER(TRIM(category))                     AS category,
    CAST(order_date          AS DATE)         AS order_date,
    CAST(ship_date           AS DATE)         AS ship_date,
    CAST(quantity_ordered    AS INTEGER)      AS quantity_ordered,
    CAST(unit_price          AS DECIMAL(10,4))AS unit_price,
    CAST(total_order_value   AS DECIMAL(14,2))AS total_order_value,
    CAST(budget_amount       AS DECIMAL(14,2))AS budget_amount,
    CAST(cost_variance_pct   AS DECIMAL(6,2)) AS cost_variance_pct,
    CAST(lead_time_days      AS INTEGER)      AS lead_time_days,
    UPPER(TRIM(po_status))                    AS po_status,
    UPPER(TRIM(approval_level))               AS approval_level,
    CAST(is_sla_met          AS BOOLEAN)      AS is_sla_met,
    etl_run_date,
    etl_load_ts

FROM source
WHERE purchase_order_id IS NOT NULL
  AND order_date         IS NOT NULL
  AND quantity_ordered   > 0
