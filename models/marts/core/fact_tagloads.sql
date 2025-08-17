{{ config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='merge',
    on_schema_change='ignore',
    partition_by={"field": "event_date", "data_type": "date"}
) }}

with
    tagloaded_events as (
        select
            event_id,
            publisher_id,
            device_id,
            page_view_id,
            event_created_at,
            event_date,
            {{ dbt_utils.generate_surrogate_key(["url","publisher_id"]) }} as article_key 
        from {{ ref('stg_events') }}
        where lower(event_name) = 'tagloaded'
        or lower(event_name) = 'mounts'
          and event_id is not null
          and event_created_at is not null
    
        {% if is_incremental() %}
        and date(event_created_at) >= coalesce(
            date_sub((select max(event_date) from {{ this }}), interval 1 day),
            date('1900-01-01')
        )
        {% endif %}
    )

select

    event_id,
    event_created_at,
    event_date,
    publisher_id,
    device_id,   
    page_view_id,
    article_key


from tagloaded_events

qualify row_number() over (
    partition by event_id
    order by event_created_at desc
) = 1