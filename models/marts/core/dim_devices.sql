-- Device dimension - rebuilt from snapshot
{{
    config(
        materialized="table",
        unique_key="device_key"
    )
}}
-- Build device dimension table
    select
    -- Core device attributes
    device_id,
    user_agent,
    device_type,
    os_type,
    browser_name,

    -- When we first/last saw this device
    first_seen,
    last_seen

    from {{ ref("int_devices") }}
    where device_id is not null



