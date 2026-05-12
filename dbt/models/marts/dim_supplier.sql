-- dbt Model: dim_supplier | SCD Type 2 via snapshot
{{
  config(materialized='table', schema='mart', dist='all',
    sort=['supplier_id','is_current'], tags=['mart','dimension'])
}}
SELECT
    {{ dbt_utils.generate_surrogate_key(['supplier_id','dbt_valid_from']) }} AS supplier_key,
    supplier_id, supplier_name, supplier_type, country, state, city,
    contact_email, contract_start_dt, contract_end_dt, payment_terms,
    preferred_flag, reliability_tier, reliability_score, is_active,
    dbt_valid_from AS effective_start_dt,
    dbt_valid_to   AS effective_end_dt,
    (dbt_valid_to IS NULL) AS is_current
FROM {{ ref('supplier_snapshot') }}
