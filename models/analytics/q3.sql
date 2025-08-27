{{ config(materialized='view') }}

-- Question 3:
-- Which product got the most impressions for each campaign?

select
    coalesce(
        c.campaign_name, 'Unknown Campaign'
    ) as campaign_name,
    p.product_name,
    count(i.product_id) as impression_count
from {{ ref("fact_impressions") }} as i

left join
    {{ ref("dim_campaigns") }} as c
    on i.brand_id = c.brand_id
    and c.current_record = true

left join
    {{ ref("dim_products") }} as p
    on i.product_id = p.product_id

group by c.campaign_name, p.product_name, c.campaign_id

qualify
    row_number() over (
        partition by c.campaign_id
        order by count(i.product_id) desc
    )
    = 1
order by impression_count desc
