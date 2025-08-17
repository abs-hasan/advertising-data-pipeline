{{ config(materialized="table", unique_key="article_key") }}


-- Build article dimension table
-- One record per unique URL with content categorization
select

    -- Surrogate key for article (based on URL)
    {{ dbt_utils.generate_surrogate_key(["url","pub.publisher_id"]) }} as article_key,

    -- Article attributes from intermediate model
    art.url,
    art.domain,
    art.section,
    art.sub_section,
    art.article_title,
    
    -- Publisher context
    pub.publisher_id,


    -- First/last time this article appeared in the event
    art.first_seen,
    art.last_seen

from {{ ref("int_articles") }} art

left join
    {{ ref("dim_publishers") }} pub on lower(art.domain) = lower(pub.publisher_domain)

