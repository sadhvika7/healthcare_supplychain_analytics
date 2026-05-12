-- dbt Model: stg_shipments
-- Layer: Staging
-- Source: Shipment curated data loaded from USAID SCMS dataset
--
-- Source URL: https://www.kaggle.com/datasets/apoorvwatsky/supply-chain-shipment-pricing-data
-- Kaggle:     https://www.kaggle.com/datasets/apoorvwatsky/supply-chain-shipment-pricing-data
--
-- Column mapping from USAID source → curated → staging:
--   id                    → shipment_id
--   vendor                → supplier_name  (supplier_id derived by hash)
--   country               → destination_country  (hospital_id derived)
--   po_sent_to_vendor_date→ ship_date
--   scheduled_delivery_date→expected_delivery_date
--   delivered_to_client_date→actual_delivery_date
--   shipment_mode         → carrier
--   product_group         → product_group_raw  (priority derived)
--   line_item_quantity    → quantity_shipped
--   line_item_value       → shipment_cost
--   unit_price            → cost_per_unit
--   freight_cost_usd      → freight_cost

{{
  config(
    materialized = 'view',
    schema       = 'staging',
    tags         = ['staging', 'shipments', 'daily']
  )
}}

WITH source AS (
    SELECT * FROM {{ source('hsc_raw', 'shipments_raw') }}
),

cleaned AS (
    SELECT
        UPPER(TRIM(shipment_id))           AS shipment_id,
        UPPER(TRIM(supplier_id))           AS supplier_id,
        UPPER(TRIM(supplier_name))         AS supplier_name,
        UPPER(TRIM(hospital_id))           AS hospital_id,
        UPPER(TRIM(destination_country))   AS destination_country,
        -- Dates already parsed in Glue job
        CAST(ship_date              AS DATE) AS ship_date,
        CAST(expected_delivery_date AS DATE) AS expected_delivery_date,
        CAST(actual_delivery_date   AS DATE) AS actual_delivery_date,
        -- Derived in Glue
        UPPER(TRIM(priority))              AS priority,
        UPPER(TRIM(carrier))               AS carrier,
        UPPER(TRIM(delivery_performance))  AS delivery_performance,
        UPPER(TRIM(product_name))          AS product_name,
        UPPER(TRIM(product_group_raw))     AS product_group,
        -- Measures
        CAST(quantity_shipped       AS INTEGER)       AS quantity_shipped,
        CAST(shipment_cost          AS DECIMAL(12,2)) AS shipment_cost,
        CAST(cost_per_unit          AS DECIMAL(10,4)) AS cost_per_unit,
        CAST(freight_cost           AS DECIMAL(12,2)) AS freight_cost,
        CAST(planned_transit_days   AS INTEGER)       AS planned_transit_days,
        CAST(actual_transit_days    AS INTEGER)       AS actual_transit_days,
        CAST(delivery_delay_days    AS INTEGER)       AS delivery_delay_days,
        CAST(sla_threshold_days     AS INTEGER)       AS sla_threshold_days,
        CAST(is_sla_met             AS BOOLEAN)       AS is_sla_met,
        CAST(is_critical            AS BOOLEAN)       AS is_critical,
        -- Audit
        etl_run_date,
        etl_load_ts

    FROM source
    WHERE shipment_id IS NOT NULL
      AND supplier_id IS NOT NULL
      AND ship_date   IS NOT NULL
      -- Remove placeholder/test records from USAID dataset
      AND UPPER(TRIM(carrier)) NOT IN ('N/A', 'TBD', 'UNKNOWN')
)

SELECT * FROM cleaned
