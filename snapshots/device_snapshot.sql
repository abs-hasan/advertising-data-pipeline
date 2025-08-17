{% snapshot dim_devices_snapshot %}

-- one row per device
-- track changes by checking certain columns
-- if these change, make a new version

{{
    config(
        target_schema='snapshots',
        unique_key=['device_id','user_agent'],
        strategy='check',           
        check_cols=['user_agent', 'device_type', 'os_type', 'browser_name'] 
    )
}}

-- pull device info from int_devices
with device_snap as (
    select
        device_id,
        user_agent,
        device_type,
        os_type,
        browser_name,
        first_seen,
        last_seen
    from {{ ref("int_devices") }}
    where device_id is not null
    group by 1, 2, 3, 4, 5,6,7
)

-- final result for snapshotting
select
    device_id,
    user_agent,
    device_type,
    os_type,
    browser_name,
    first_seen,
    last_seen
from device_snap

{% endsnapshot %}