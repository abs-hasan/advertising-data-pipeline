{{ config(materialized='table') }}

-- Simple date dimension using BigQuery built-ins
select
    format_date('%Y%m%d', date_day) as date_key,
    date_day as date_actual,
    extract(year from date_day) as year,
    extract(month from date_day) as month_number,
    extract(day from date_day) as day,
    extract(dayofweek from date_day) as day_of_week,
    date_trunc(date_day, week(monday)) as week_beginning_date,
    format_date('%A', date_day) as day_name,
    extract(dayofweek from date_day) in (1, 7) as is_weekend
from unnest(generate_date_array('2025-05-01', '2025-05-31')) as date_day
order by date_actual