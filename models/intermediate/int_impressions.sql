{{ config(materialized="view") }}

-- Build intermediate product imprressions data

select
    -- Event details
    event_id,
    publisher_id,
    device_id,
    url,
    page_view_id,

    -- Timestamp fields 
    event_created_at as impression_timestamp,
    date(event_created_at) as impression_date,

    -- Product details from the array
    product.element.product_id,
    product.element.brand_id,
    product.element.product_name,
    safe_cast(product.element.product_price as float64) as price_at_impression,
    product.element.product_url,
    product.element.product_image_url

from {{ ref("stg_events") }}
cross join unnest(products.list) as product
where
    -- Filter to impression events only
    lower(event_name) = 'productimpressions'

    -- Ensure we have valid product data
    and products.list is not null
    and array_length(products.list) > 0
    and product.element.product_id is not null

-- keep only the latest record per (event_id, product.element.product_id)
qualify
    row_number() over (
        partition by event_id, product.element.product_id
        order by event_created_at desc, device_id desc
    )
    = 1
