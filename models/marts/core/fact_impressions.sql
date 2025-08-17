-- Build as an incremental table for efficiency
{{
    config(
        materialized="incremental",
        unique_key="impression_key",
        incremental_strategy="merge",
        on_schema_change="ignore",
        partition_by={"field": "impression_date", "data_type": "date"},
    )
}}

with
    impression_final as (
        select
            -- Primary key: surrogate key 
            {{ dbt_utils.generate_surrogate_key(["event_id", "product_id"]) }} as impression_key,
            {{ dbt_utils.generate_surrogate_key(["url","publisher_id"]) }} as article_key ,
            
            -- base fields
            event_id,
            product_id,
            brand_id,
            publisher_id,
            device_id,
            page_view_id,
            
            -- Timestamp fields
            impression_timestamp,
            impression_date,
            
            
            -- price info
            price_at_impression

        from {{ ref("int_impressions") }}
        where
            -- Data quality filters
            event_id is not null 
            and product_id is not null

            -- Incremental loading logic
            {% if is_incremental() %}

            -- Process data from 1 day before max date to handle late arrivals
                and date(impression_timestamp) >= coalesce(
                    date_sub(
                        (select max(impression_date) from {{ this }}), interval 1 day),
                    date('1900-01-01')) -- Fallback for initial full load
            {% endif %}
    )
select
    -- Final selected columns, with foreign keys for the star schema
    impression_key,
    impression_timestamp,
    impression_date,

    event_id,
    product_id,
    brand_id,
    publisher_id,
    device_id,
    article_key,
    page_view_id,

    price_at_impression

from impression_final
