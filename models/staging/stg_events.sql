{{ config(materialized="view") }}

with
    -- keep events that have an ID and a timestamp
    source_events as (
        select
            cast(event_created_at as datetime) as event_timestamp,
            event_name,
            event_context,
            event_data
        from {{ source("raw_trendii", "stg_events_ext") }}
        where event_context.eid is not null and event_created_at is not null
    )
select
    event_timestamp as event_created_at,
    date(event_timestamp) as event_date,
    event_name,

    -- fallback to 'Unknown' to avoid nulls downstream
    coalesce(event_context.eid, 'Unknown') as event_id,
    coalesce(event_context.did, 'Unknown') as device_id,

    -- Normalize domain: lowercase and strip
    coalesce(regexp_replace(lower(event_context.domain), r'^www\.', ''), 'Unknown') as domain,

    -- Keep publisher_id as string for consistent joins
    coalesce(safe_cast(event_context.publisher_id as string), 'Unknown') as publisher_id,
    coalesce(event_context.pvid, 'Unknown') as page_view_id,

    event_context.ua as user_agent,
    event_context.url,

    -- Event Data columns
    event_data.brand_id,
    event_data.click_id,
    event_data.image_id,
    event_data.product_id,
    event_data.product_image_url,
    event_data.product_name,
    event_data.product_price,
    event_data.product_url,
    event_data.mounts,
    event_data.products,

from source_events
