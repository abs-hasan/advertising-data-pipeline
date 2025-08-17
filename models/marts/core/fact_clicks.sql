{{
    config(
        materialized="incremental",
        unique_key="product_click_key",
        incremental_strategy="merge",
        on_schema_change="ignore",
        partition_by={"field": "click_date", "data_type": "date"},
    )
}}

-- Build product click fact table

with
    click_final as (
        select
    
            -- Primary key: surrogate key 
            {{dbt_utils.generate_surrogate_key(["event_id", "click_id", "product_id"])}} as product_click_key,
            {{ dbt_utils.generate_surrogate_key(["url","publisher_id"]) }} as article_key ,

            event_id,
            publisher_id,
            device_id,
            page_view_id,
            click_date,
            click_timestamp,
            brand_id,
            click_id,
            image_id,
            product_id,

            price_at_click

        from {{ ref("int_product_clicks") }}
        where
        
            -- Data quality filters
            event_id is not null
            and product_id is not null

             -- Incremental loading logic
            {% if is_incremental() %}

            -- Process data from 1 day before max date to handle late arrivals
                and click_date >= coalesce(
                    date_sub((select max(click_date) from {{ this }}), interval 1 day),
                    date('1900-01-01')
                )
            {% endif %}
    )

select 
    product_click_key,
 
    click_date,
    click_timestamp,
    event_id,
    page_view_id,
    click_id,
    image_id,
    product_id,
    brand_id,
    publisher_id,
    device_id,
    article_key,

    price_at_click

from click_final

