{{ config(materialized='table') }}

-- Build date dimension table with Monday week start

with date_spine as (
    -- Generate dates from your data start to 30 days in future
    select date_add('2025-05-01', interval day_offset day) as date_day
    from unnest(generate_array(0, 30)) as day_offset
),

date_details as (
    select
        -- Primary key for joins
        format_date('%Y%m%d', date_day) as date_key,
        date_day as date_actual,
        
        -- Essential components only
        extract(year from date_day) as year,
        extract(month from date_day) as month_number,
        extract(day from date_day) as day,
        extract(dayofweek from date_day) as day_of_week,
        
        -- Week beginning (Monday start) - FIXED CALCULATION
        date_sub(date_day, interval 
            case extract(dayofweek from date_day)
                when 1 then 6  -- Sunday: go back 6 days to Monday
                when 2 then 0  -- Monday: no change
                when 3 then 1  -- Tuesday: go back 1 day
                when 4 then 2  -- Wednesday: go back 2 days
                when 5 then 3  -- Thursday: go back 3 days
                when 6 then 4  -- Friday: go back 4 days
                when 7 then 5  -- Saturday: go back 5 days
            end day
        ) as week_beginning_date,
        
        -- Day name
        case extract(dayofweek from date_day)
            when 1 then 'Sunday'
            when 2 then 'Monday'
            when 3 then 'Tuesday'
            when 4 then 'Wednesday'
            when 5 then 'Thursday'
            when 6 then 'Friday'
            when 7 then 'Saturday'
        end as day_name,
        
        -- Weekend flag
        case 
            when extract(dayofweek from date_day) in (1, 7) then true
            else false
        end as is_weekend

    from date_spine
)

select * from date_details
order by date_actual