{{ config(materialized='view') }}

-- TRENDii Analysis Question 3:
-- Which product got the most impressions for each campaign?

SELECT 
  COALESCE(c.campaign_name, 'Unknown Campaign') as campaign_name,
  p.product_name,
  COUNT(i.product_id) as impression_count
FROM {{ ref('fact_impressions') }} as i
LEFT JOIN {{ ref('dim_campaigns') }} as c
  ON i.brand_id = c.brand_id AND c.current_record = true
LEFT JOIN {{ ref('dim_products') }} as p
  ON i.product_id = p.product_id
GROUP BY c.campaign_name, p.product_name, c.campaign_id
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY c.campaign_id 
  ORDER BY COUNT(i.product_id) DESC
) = 1
ORDER BY impression_count DESC