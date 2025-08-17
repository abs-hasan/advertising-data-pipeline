{{ config(materialized="view") }}

-- TRENDii Analysis Question 2:
-- Top 3 clicked products for each brand in the final week (using dim_dates)
select
    coalesce(camp.company_name, 'Unknown Company') as company_name,
    p.product_name,
    count(*) as click_count
from {{ ref("fact_clicks") }} c
join {{ ref("dim_dates") }} d on c.click_date = d.date_actual
join {{ ref("dim_products") }} p on c.product_id = p.product_id
left join
    {{ ref("dim_campaigns") }} camp
    on c.brand_id = camp.brand_id
    and camp.current_record = true
where
    d.week_beginning_date = (
        select max(d2.week_beginning_date)
        from {{ ref("fact_clicks") }} c2
        join {{ ref("dim_dates") }} d2 on c2.click_date = d2.date_actual
    )
group by camp.company_name, p.product_name, c.brand_id
qualify
    row_number() over (partition by c.brand_id order by count(*) desc, p.product_name)
    <= 3
order by click_count desc, company_name, product_name
