{{ config(materialized="table", unique_key="product_id") }}


-- Build product dimension table 
select
 

    -- Core product columns
    product_id,
    brand_id,
    sku,

    -- Basic product info
    product_name,
    product_url,
    image_url,

    -- Pricing info
    price,
    sale_price,

    -- Extra fields to track sales/discounts
    created_at,
    on_sale,
    discount_percentage

from {{ ref("stg_products") }}
