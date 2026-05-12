{% snapshot supplier_snapshot %}
{{
    config(
      target_schema  = 'snapshots',
      unique_key     = 'supplier_id',
      strategy       = 'check',
      check_cols     = ['reliability_tier','reliability_score','is_active','contact_email'],
    )
}}
-- Source: Synthetic supplier master dataset
-- Tracks SCD Type 2 changes to reliability tier, score, and active status
SELECT
    supplier_id, supplier_name, supplier_type,
    country, state, city, contact_email,
    contract_start_dt, contract_end_dt, payment_terms,
    preferred_flag, reliability_tier, reliability_score, is_active,
    CURRENT_TIMESTAMP AS snapshot_ts
FROM {{ source('hsc_raw', 'suppliers_raw') }}
{% endsnapshot %}
