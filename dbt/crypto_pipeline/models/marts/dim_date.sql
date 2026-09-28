{{
    config(
        materialized='table'
    )
}}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2026-01-01' as date)",
        end_date="cast('2026-12-31' as date)"
    ) }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['date_day']) }} AS date_key,
    date_day AS full_date,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(MONTH FROM date_day) AS month,
    EXTRACT(DAY FROM date_day) AS day,
    EXTRACT(DAYOFWEEK FROM date_day) AS day_of_week,
    FORMAT_DATE('%A', date_day) AS day_name,
    FORMAT_DATE('%B', date_day) AS month_name
FROM date_spine
