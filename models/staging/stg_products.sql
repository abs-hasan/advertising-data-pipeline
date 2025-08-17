{{ config(materialized='view') }}

    select
        id as product_id,
        brand_id,
        sku,
        name as product_name,
        product_url,
        image_url,
        price,
        sale_price,
        created_at,
        
        -- if product is on sale (sale_price exists)
        case when sale_price is not null 
            then true else false 
        end as on_sale,
        
        -- Calculate discount percentage when on sale
        case when sale_price is not null 
             then round((price - sale_price) / price * 100, 2) else 0.0 
        end as discount_percentage
        
from {{ source('raw_trendii', 'dim_product_ext') }}

-- Ensure only valid products with IDs are included
where id is not null