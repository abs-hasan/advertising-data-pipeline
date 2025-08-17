{{ config(materialized='view') }}

-- TRENDii Analysis Question 2:
-- Top 3 clicked products for each brand in the final week (using dim_dates)

SELECT
  COALESCE(camp.company_name, 'Unknown Company') AS company_name,
  p.product_name,
  COUNT(*) AS click_count  -- FIXED: Added *
FROM {{ ref('fact_clicks') }} c
JOIN {{ ref('dim_dates') }} d
  ON c.click_date = d.date_actual
JOIN {{ ref('dim_products') }} p
  ON c.product_id = p.product_id
LEFT JOIN {{ ref('dim_campaigns') }} camp
  ON c.brand_id = camp.brand_id AND camp.current_record = true
WHERE d.week_beginning_date = (
  SELECT MAX(d2.week_beginning_date)
  FROM {{ ref('fact_clicks') }} c2
  JOIN {{ ref('dim_dates') }} d2
    ON c2.click_date = d2.date_actual
)
GROUP BY camp.company_name, p.product_name, c.brand_id
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY c.brand_id
  ORDER BY COUNT(*) DESC, p.product_name  
) <= 3
ORDER BY click_count DESC, company_name, product_name