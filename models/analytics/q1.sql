{{ config(materialized="view") }}

-- Top 5 articles by traffic per domain
select
    art.domain,
    art.article_title,
    count(distinct tag.page_view_id) as traffic_count
from {{ ref("fact_tagloads") }} tag

left join
    {{ ref("dim_articles") }} art
    on tag.article_key = art.article_key

where art.domain is not null
group by art.domain, art.article_title

qualify
    row_number() over (
        partition by art.domain
        order by count(distinct tag.page_view_id) desc
    )
    <= 5
order by art.domain, traffic_count desc
