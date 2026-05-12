-- dbt Model: dim_date | 2006-01-01 to 2030-12-31
-- Start date matches earliest USAID dataset records
{{
  config(materialized='table', schema='mart', dist='all', sort=['full_date'])
}}
WITH spine AS (
    {{ dbt_utils.date_spine(datepart="day",
       start_date="cast('2006-01-01' as date)",
       end_date="cast('2030-12-31' as date)") }}
)
SELECT
    CAST(TO_CHAR(date_day,'YYYYMMDD') AS INTEGER) AS date_key,
    date_day AS full_date,
    EXTRACT(YEAR FROM date_day)::SMALLINT    AS year,
    EXTRACT(QUARTER FROM date_day)::SMALLINT AS quarter,
    EXTRACT(MONTH FROM date_day)::SMALLINT   AS month,
    TO_CHAR(date_day,'Month')                AS month_name,
    EXTRACT(WEEK FROM date_day)::SMALLINT    AS week_of_year,
    EXTRACT(DAY FROM date_day)::SMALLINT     AS day_of_month,
    EXTRACT(DOW FROM date_day)::SMALLINT     AS day_of_week,
    TO_CHAR(date_day,'Day')                  AS day_name,
    EXTRACT(DOW FROM date_day) IN (0,6)      AS is_weekend
FROM spine
