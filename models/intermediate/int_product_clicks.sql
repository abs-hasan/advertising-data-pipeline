{{ config(materialized="view") }}

-- Build intermediate product click data
select

    event_id,
    publisher_id,
    device_id,
    page_view_id,
    event_created_at,
    event_created_at as click_timestamp,
    date(event_created_at) as click_date,

    brand_id,
    click_id,
    image_id,
    product_id,
    product_image_url,
    product_name,
    url,

    -- Pricing at time of click
    safe_cast(product_price as float64) as price_at_click,

    product_url

from {{ ref("stg_events") }}
where
    -- Filter to click events only
    lower(event_name) = 'productclick'

    -- Ensure we have valid product data
    and event_created_at is not null
    and click_id is not null
    and product_id is not null

-- keep only the latest record per (event_id, click_id, product_id)
qualify
    row_number() over (
        partition by event_id, click_id, product_id 
        order by click_timestamp desc, page_view_id desc, event_id desc
        )= 1
