{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH date_range AS (
    SELECT 
        generate_series(
            '2024-01-01'::date,
            '2026-12-31'::date,
            '1 day'::interval
        )::date as full_date
),

date_features AS (
    SELECT
        {{ dbt_utils.surrogate_key(['full_date']) }} as date_key,
        full_date,
        EXTRACT(YEAR FROM full_date) as year,
        EXTRACT(QUARTER FROM full_date) as quarter,
        EXTRACT(MONTH FROM full_date) as month,
        TO_CHAR(full_date, 'Month') as month_name,
        EXTRACT(WEEK FROM full_date) as week_of_year,
        EXTRACT(DAY FROM full_date) as day_of_month,
        EXTRACT(DOW FROM full_date) as day_of_week,
        TO_CHAR(full_date, 'Day') as day_name,
        EXTRACT(DOY FROM full_date) as day_of_year,
        CASE 
            WHEN EXTRACT(DOW FROM full_date) IN (0, 6) THEN TRUE
            ELSE FALSE
        END as is_weekend,
        CASE 
            WHEN EXTRACT(MONTH FROM full_date) = 1 AND EXTRACT(DAY FROM full_date) = 1 THEN 'New Year'
            WHEN EXTRACT(MONTH FROM full_date) = 12 AND EXTRACT(DAY FROM full_date) = 25 THEN 'Christmas'
            ELSE 'Normal Day'
        END as holiday_flag,
        CURRENT_TIMESTAMP as loaded_at
    FROM date_range
)

SELECT * FROM date_features