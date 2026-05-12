-- dbt Model: kpi_operational_summary
-- Unified daily KPI table consumed by Power BI dashboards
{{
  config(materialized='table', schema='kpi',
    sort=['kpi_date','hospital_key'], dist='hospital_key',
    tags=['kpi','dashboard','daily'])
}}
WITH inv AS (
    SELECT
        record_date AS kpi_date, hospital_key,
        COUNT(inventory_id) AS total_skus,
        SUM(CASE WHEN stock_status='STOCKOUT' THEN 1 ELSE 0 END) AS stockout_count,
        SUM(CASE WHEN is_critical_shortage THEN 1 ELSE 0 END)    AS critical_shortage_count,
        SUM(quantity_on_hand)   AS total_units_on_hand,
        SUM(inventory_value)    AS total_inventory_value,
        ROUND(100.0*(COUNT(inventory_id) - SUM(CASE WHEN stock_status='STOCKOUT' THEN 1 ELSE 0 END))
              / NULLIF(COUNT(inventory_id),0), 2) AS inventory_fill_rate_pct,
        ROUND(100.0*SUM(CASE WHEN stock_status='STOCKOUT' THEN 1 ELSE 0 END)
              / NULLIF(COUNT(inventory_id),0), 2) AS stockout_pct
    FROM {{ ref('fact_inventory') }}
    GROUP BY 1,2
),
ship AS (
    SELECT
        ship_date AS kpi_date, hospital_key,
        COUNT(shipment_id) AS total_shipments,
        SUM(CASE WHEN is_sla_met THEN 1 ELSE 0 END)      AS on_time_count,
        SUM(CASE WHEN NOT is_sla_met THEN 1 ELSE 0 END)  AS delayed_count,
        ROUND(AVG(actual_transit_days),2)                 AS avg_delivery_days,
        ROUND(100.0*SUM(CASE WHEN is_sla_met THEN 1 ELSE 0 END)
              / NULLIF(COUNT(shipment_id),0), 2)          AS shipment_sla_pct,
        SUM(shipment_cost)  AS total_shipment_cost
    FROM {{ ref('fact_shipments') }}
    WHERE actual_delivery_date IS NOT NULL
    GROUP BY 1,2
)
SELECT
    COALESCE(i.kpi_date,   s.kpi_date)   AS kpi_date,
    COALESCE(i.hospital_key,s.hospital_key) AS hospital_key,
    i.total_skus, i.stockout_count, i.critical_shortage_count,
    i.total_units_on_hand, i.total_inventory_value,
    i.inventory_fill_rate_pct, i.stockout_pct,
    s.total_shipments, s.on_time_count, s.delayed_count,
    s.avg_delivery_days, s.shipment_sla_pct, s.total_shipment_cost,
    ROUND(
        COALESCE(i.inventory_fill_rate_pct,0)*0.40
      + COALESCE(s.shipment_sla_pct,0)*0.60,
    2) AS operational_health_score,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM inv i
FULL OUTER JOIN ship s ON i.kpi_date=s.kpi_date AND i.hospital_key=s.hospital_key
