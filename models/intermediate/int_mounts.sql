{{ config(materialized="view") }}

-- Build intermediate mount data
select
    -- Event details
    event_id,
    publisher_id,
    device_id,
    page_view_id,
    domain,
    url,

    -- Timestamp fields 
    event_created_at as mount_timestamp,
    date(event_created_at) as mount_date,

    -- Mount details from the array
    mount.element.image_id,
    safe_cast(mount.element.mount_index as int64) as mount_index

from {{ ref("stg_events") }}
cross join unnest(mounts.list) as mount
where
    -- Filter to mounts events only
    lower(event_name) = 'mounts'
    and array_length(mounts.list) > 0
    and event_created_at is not null
    and mount.element.image_id is not null

-- keep only the latest record
qualify
    row_number() over (
        partition by event_id, mount.element.image_id, mount.element.mount_index 
        order by mount_timestamp desc)= 1
