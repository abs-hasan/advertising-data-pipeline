{{ config(materialized="view") }}

with
    user_agent_clean as (
        select
            device_id,
            coalesce(user_agent, '') as ua_clean,
            min(event_created_at) as first_seen,
            max(event_created_at) as last_seen
        from {{ ref("stg_events") }}
        where device_id is not null
        group by device_id, user_agent
    )

select
    device_id,
    ua_clean as user_agent,

    -- Detect if device is mobile using common mobile indicators
    case
        when regexp_contains(ua_clean, r'(?i)\b(iPad|Tablet)\b') then 'Tablet'
        when regexp_contains(ua_clean, r'(?i)\bMobi\b') then 'Mobile'
        when regexp_contains(ua_clean, r'(?i)\bDalvik\/') then 'Mobile'  
        else 'Desktop'
    end as device_type,

    -- Detect WebView vs native browsers (important for attribution)
    case
        when regexp_contains(ua_clean, r'(?i)\(.*Android') then 'Android'
        when regexp_contains(ua_clean, r'(?i)\(.*iPhone') then 'iOS'
        when regexp_contains(ua_clean, r'(?i)\(.*iPad') then 'iPadOS'
        when regexp_contains(ua_clean, r'(?i)\(.*Macintosh') then 'macOS'
        when regexp_contains(ua_clean, r'(?i)\(.*Windows') then 'Windows'
        when regexp_contains(ua_clean, r'(?i)\(.*Linux') then 'Linux'
        else 'Other/Unknown'
    end as os_type,

    case
        when regexp_contains(ua_clean, r'(?i)\(.*;\s*wv\)') then 'Android WebView'
        when
            regexp_contains(ua_clean, r'(?i)AppleWebKit')
            and regexp_contains(ua_clean, r'(?i)\bMobile\b')
            and not regexp_contains(
                ua_clean,
                r'(?i)(Safari/|Chrome/|CriOS/|Firefox/|FxiOS/|Edg/|OPR/|Opera/)'
            )
        then 'iOS WebView'

 
        when regexp_contains(ua_clean, r'(?i)Seamonkey/[0-9.]+') then 'SeaMonkey'
        when
            regexp_contains(ua_clean, r'(?i)Firefox/[0-9.]+')
            and not regexp_contains(ua_clean, r'(?i)Seamonkey/')  then 'Firefox'
        when regexp_contains(ua_clean, r'(?i)Chromium/[0-9.]+')
        then 'Chromium'
        when regexp_contains(ua_clean, r'(?i)OPR/[0-9.]+|Opera/[0-9.]+')
        then 'Opera'
        when regexp_contains(ua_clean, r'(?i)Edg/[0-9.]+')
        then 'Edge'
        when
            regexp_contains(ua_clean, r'(?i)(CriOS|Chrome)/[0-9.]+')
            and not regexp_contains(ua_clean, r'(?i)(Chromium/|Edg/|OPR/|Opera/)')
        then 'Chrome'
        when
            regexp_contains(ua_clean, r'(?i)Safari/[0-9.]+')
            and not regexp_contains(
                ua_clean, r'(?i)(Chrome/|CriOS/|Chromium/|Edg/|OPR/|Opera/)'
            )
        then 'Safari'
        else 'Unknown'
    end as browser_name,
    first_seen,
    last_seen

from user_agent_clean
