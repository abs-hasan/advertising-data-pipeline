{{ config(materialized="view") }}

-- Top 3 clicked products per brand in final week
select
    coalesce(
        camp.company_name, 'Unknown Company'
    ) as company_name,
    p.product_name,
    count(*) as click_count
from {{ ref("fact_clicks") }} c
join {{ ref("dim_dates") }} d on c.click_date = d.date_actual
join {{ ref("dim_products") }} p on c.product_id = p.product_id

left join
    {{ ref("dim_campaigns") }} camp
    on c.brand_id = camp.brand_id
    and camp.current_record

where
    d.week_beginning_date = (
        select max(week_beginning_date)
        from {{ ref("dim_dates") }}
    )

group by 1, 2, c.brand_id

qualify
    row_number() over (
        partition by c.brand_id order by count(*) desc
    )
    <= 3
order by click_count desc, company_name
