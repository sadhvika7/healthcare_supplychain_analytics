-- dbt Model: fact_shipments | Incremental Mart
-- Source: USAID SCMS Shipment Pricing Data via stg_shipments
{{
  config(materialized='incremental', unique_key='shipment_id',
    schema='mart', dist='supplier_key', sort=['ship_date_key','hospital_key'],
    tags=['mart','fact','shipments'], on_schema_change='sync_all_columns')
}}
WITH shipments AS (
    SELECT * FROM {{ ref('stg_shipments') }}
    {% if is_incremental() %}
    WHERE ship_date > (SELECT COALESCE(MAX(ship_date),'2006-01-01') FROM {{ this }})
    {% endif %}
),
dim_date     AS (SELECT date_key, full_date FROM {{ ref('dim_date') }}),
dim_hospital AS (SELECT hospital_key, hospital_id FROM {{ ref('dim_hospital') }} WHERE is_active),
dim_product  AS (SELECT product_key, product_name FROM {{ ref('dim_product') }}),
dim_supplier AS (SELECT supplier_key, supplier_id FROM {{ ref('dim_supplier') }} WHERE is_current)
SELECT
    d_ship.date_key AS ship_date_key, d_del.date_key AS delivery_date_key,
    dsupp.supplier_key, dhosp.hospital_key, dprod.product_key,
    s.shipment_id, s.quantity_shipped, s.shipment_cost, s.cost_per_unit,
    s.freight_cost, s.planned_transit_days, s.actual_transit_days,
    s.delivery_delay_days, s.sla_threshold_days, s.priority, s.carrier,
    s.delivery_performance, s.is_sla_met, s.is_critical,
    s.ship_date, s.expected_delivery_date, s.actual_delivery_date,
    s.etl_load_ts, CURRENT_TIMESTAMP AS dbt_updated_at
FROM shipments s
LEFT JOIN dim_date     d_ship ON s.ship_date            = d_ship.full_date
LEFT JOIN dim_date     d_del  ON s.actual_delivery_date = d_del.full_date
LEFT JOIN dim_supplier dsupp  ON s.supplier_id          = dsupp.supplier_id
LEFT JOIN dim_hospital dhosp  ON s.hospital_id          = dhosp.hospital_id
LEFT JOIN dim_product  dprod  ON s.product_name         = dprod.product_name
