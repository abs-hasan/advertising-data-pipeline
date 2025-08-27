{{ config(materialized="view") }}

-- Mount rate per domain
select
    a.domain,
    countif(event_type = 'mount') as total_mounts,
    countif(event_type = 'tagload') as total_tagloads,
    round(
        safe_divide(
            countif(event_type = 'mount'),
            countif(event_type = 'tagload')
        ),
        4
    ) as mount_rate
from

    (
        select article_key, 'mount' as event_type
        from {{ ref("fact_mounts") }}
        union all
        select article_key, 'tagload' as event_type
        from {{ ref("fact_tagloads") }}
    ) events
join

    {{ ref("dim_articles") }} a
    on events.article_key = a.article_key
group by a.domain
having total_tagloads > 0
order by mount_rate desc
