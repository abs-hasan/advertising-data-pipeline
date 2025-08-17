{{ config(materialized='view') }}

-- TRENDii Analysis Question 1:
-- What were the top five articles by traffic per domain?

WITH ranked_articles AS (
    SELECT 
        art.domain,
        art.article_title,
        COUNT(DISTINCT tag.page_view_id) as traffic_count,
        ROW_NUMBER() OVER (
            PARTITION BY art.domain 
            ORDER BY COUNT(DISTINCT tag.page_view_id) DESC
        ) as rank
    FROM {{ ref('fact_tagloads') }} as tag
    LEFT JOIN {{ ref('dim_articles') }} as art 
        ON tag.article_key = art.article_key
    WHERE art.domain IS NOT NULL
    GROUP BY art.domain, art.article_title
)

SELECT 
    domain,
    article_title,
    traffic_count
FROM ranked_articles
WHERE rank <= 5
ORDER BY domain, traffic_count DESC