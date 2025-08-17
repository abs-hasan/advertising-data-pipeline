{{ config(materialized='view') }}

-- TRENDii Analysis Question 4:
-- What is the mount rate for each domain? (mounts / tagloads)

WITH domain_events AS (
  SELECT 
    a.domain,
    'mount' as event_type,
    COUNT(*) as event_count
  FROM {{ ref('fact_mounts') }} m
  JOIN {{ ref('dim_articles') }} a
    ON m.article_key = a.article_key
  GROUP BY a.domain
  
  UNION ALL
  
  SELECT 
    a.domain,
    'tagload' as event_type,
    COUNT(*) as event_count
  FROM {{ ref('fact_tagloads') }} t
  JOIN {{ ref('dim_articles') }} a
    ON t.article_key = a.article_key
  GROUP BY a.domain
)

SELECT 
  domain,
  SUM(CASE WHEN event_type = 'mount' THEN event_count ELSE 0 END) as total_mounts,
  SUM(CASE WHEN event_type = 'tagload' THEN event_count ELSE 0 END) as total_tagloads,
  CASE 
    WHEN SUM(CASE WHEN event_type = 'tagload' THEN event_count ELSE 0 END) > 0 
    THEN ROUND(
      SUM(CASE WHEN event_type = 'mount' THEN event_count ELSE 0 END) / 
      SUM(CASE WHEN event_type = 'tagload' THEN event_count ELSE 0 END), 
      4
    )
    ELSE 0.0
  END as mount_rate
FROM domain_events
GROUP BY domain
HAVING SUM(CASE WHEN event_type = 'tagload' THEN event_count ELSE 0 END) > 0
ORDER BY mount_rate DESC, domain