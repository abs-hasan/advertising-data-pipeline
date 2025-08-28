{{ config(materialized="view") }}

-- get section / sub-section / article title from  URLs.
-- We also keep the first/last time we saw each URL.


with
    path_stage as (
        -- One row per (url, domain) with first/last seen
        select url,
        domain,
        min(event_created_at) as first_seen,
        max(event_created_at) as last_seen,
        lower(url) as url_lc,
        -- Extract the path part after the domain from URLs
        regexp_extract(lower(url), r'^[^/]+(/.*)$') as path_only
        from {{ ref("stg_events") }}
        where url is not null
        group by 1, 2
    ),

    parts_clean as (
        -- Split the path into clean segments (drop empties)
        select
            url,
            url_lc,
            domain,
            first_seen,
            last_seen,
            array(
                select seg
                from unnest(split(coalesce(path_only, '/'), '/')) seg
                where seg is not null and seg != ''
            ) as path_parts
        from path_stage
    ),

    final as (
        -- Derive section, sub-section, and a readable article slug
        select
            url,
            url_lc,
            domain,
            first_seen,
            last_seen,
            -- first segment after domain
            coalesce(path_parts[safe_offset(0)], 'home') as section_a,
             -- second segment after domain
            coalesce(path_parts[safe_offset(1)], 'general') as sub_section_a,

            (
                -- Find the best article title by avoiding IDs and preferring meaningful slugs
                select slug
                from unnest(path_parts) as slug
                with
                offset as pos
                qualify
                    row_number() over (
                        order by
                            case
                                when regexp_contains(slug, r'^[0-9a-f\-]{20,}$') -- Skip long hex IDs like abcd123
                                then 2
                                when regexp_contains(slug, r'^[0-9]{6,}$') -- Skip numeric IDs  1234
                                then 1
                                else 0
                            end,
                            - pos -- Prefer later segments
                    )
                    = 1
            ) as article_slug
        from parts_clean
    )
-- Final tidy output for analytics
select
    url,
    domain,
    initcap(replace(section_a, '-', ' ')) as section,
    initcap(replace(sub_section_a, '-', ' ')) as sub_section,
    initcap(replace(article_slug, '-', ' ')) as article_title,
    first_seen,
    last_seen
    
from final
