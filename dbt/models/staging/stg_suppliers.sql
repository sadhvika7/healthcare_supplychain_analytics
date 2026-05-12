-- dbt Model: stg_suppliers
-- Source: Synthetic supplier master (generated via datasets/generate_suppliers.py)
-- Schema mirrors real vendor master data structure

{{
  config(
    materialized = 'view',
    schema       = 'staging',
    tags         = ['staging', 'suppliers']
  )
}}

WITH source AS (
    SELECT * FROM {{ source('hsc_raw', 'suppliers_raw') }}
)

SELECT
    UPPER(TRIM(supplier_id))                   AS supplier_id,
    UPPER(TRIM(supplier_name))                 AS supplier_name,
    UPPER(TRIM(supplier_type))                 AS supplier_type,
    UPPER(TRIM(country))                       AS country,
    UPPER(TRIM(state))                         AS state,
    UPPER(TRIM(city))                          AS city,
    LOWER(TRIM(contact_email))                 AS contact_email,
    CAST(contract_start_dt  AS DATE)           AS contract_start_dt,
    CAST(contract_end_dt    AS DATE)           AS contract_end_dt,
    UPPER(TRIM(payment_terms))                 AS payment_terms,
    CAST(preferred_flag     AS BOOLEAN)        AS preferred_flag,
    UPPER(TRIM(reliability_tier))              AS reliability_tier,
    CAST(reliability_score  AS DECIMAL(5,2))   AS reliability_score,
    CAST(is_active          AS BOOLEAN)        AS is_active

FROM source
WHERE supplier_id   IS NOT NULL
  AND supplier_name IS NOT NULL
  AND supplier_type IN ('DISTRIBUTOR','MANUFACTURER','GPO','WHOLESALER')
