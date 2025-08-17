{{ config(materialized='view') }}

-- TRENDii Analysis Question 5:
-- How many unique users were advertised to?

SELECT 
  COUNT(DISTINCT device_id) as unique_users_advertised
FROM {{ ref('fact_impressions') }}
WHERE device_id IS NOT NULL