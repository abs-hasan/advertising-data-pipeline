-- Build as an incremental table for efficiency
{{
    config(
        materialized="incremental",
        unique_key="mount_key",
        incremental_strategy="merge",
        partition_by={"field": "mount_date", "data_type": "date"},
    )
}}


with
    mount_final as (
        select
             -- Primary key: surrogate key 
            {{ dbt_utils.generate_surrogate_key( ["event_id", "image_id", "mount_index"] ) }} as mount_key,
            event_id,
            publisher_id,
            device_id,
            page_view_id,
            mount_date,
            mount_timestamp,
            image_id,
            mount_index,
            {{ dbt_utils.generate_surrogate_key(["url","publisher_id"]) }} as article_key ,

           -- Lower number means higher up the page
            case
                when mount_index <= 1 then 'Better'
                when mount_index <= 3 then 'High Visibility'
                when mount_index <= 10 then 'Medium Visibility'
                else 'Low'
            end as visibility_category

        from {{ ref("int_mounts") }}
        where

            -- Data quality filters
            event_id is not null 
            and image_id is not null 
           
            -- Incremental loading logic
            {% if is_incremental() %}

            -- Process data from 1 day before max date to handle late arrivals
                and mount_date >= coalesce(
                    date_sub((select max(mount_date) from {{ this }}), interval 1 day),
                    date('1900-01-01') ) -- Fallback for initial full load
            {% endif %}

    )
select

    mount_key, 
    mount_date,
    mount_timestamp,
    event_id,
    page_view_id,
    publisher_id,
    device_id,
    article_key,
    image_id,
    mount_index,
    
    visibility_category

from mount_final
