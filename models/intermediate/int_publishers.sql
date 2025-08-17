{{ config(materialized="view") }}


-- Build intermediate publisher data
-- pull publishers from events, attach a standardised name, and track first/last seen.

select

    publisher_id,
    domain as publisher_domain,
    coalesce(cs.standard_company_name, '') as publisher_name, -- use standard name from seed
    min(event_created_at) as first_seen,
    max(event_created_at) as last_seen

from {{ ref("stg_events") }} p

left join
    {{ ref("company_standardization") }} cs
    on lower(p.domain) = lower(cs.company_domain)

where publisher_id is not null

group by 1, 2, 3
