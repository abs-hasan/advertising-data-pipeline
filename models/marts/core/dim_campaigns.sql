{{ config (materialized = "table") }}

-- Build campaign dimension table
-- Each row represents a campaign version (changes tracked by valid_from/valid_to)

select
 

    -- Core campaign attributes
    campaign_id,
    brand_id,
    campaign_name,
    product_type,

    -- Pricing / cost metrics
    cpc_rate,
    cmp_rate,
    cpa_percentage,

    -- Company details
    company_name,
    company_domain,
    
    -- version tracking
    created_at,
    valid_from,
    valid_to,
    current_record

from {{ ref("stg_campaigns") }}
