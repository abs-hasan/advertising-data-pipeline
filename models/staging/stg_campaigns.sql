{{ config(materialized="view") }}

select
    safe_cast(id as string) as campaign_id,
    brand_id,
    coalesce(name, "Unknown") as campaign_name,

    -- Product type based on available rate fields
     -- 1) Use existing product_type if given
    -- 2) If more than one rate field is filled → mark as INVALID
    -- 3) Otherwise infer type from whichever single field is present
    -- 4) Fallback to null if nothing applies
    case
        when product_type is not null then upper(product_type)
        when
            if(cpc_rate is not null, 1, 0)
            + if(safe_cast(cpm_rate as float64) is not null, 1, 0)
            + if(cpa_percentage is not null, 1, 0)
            > 1
        then 'INVALID'
        when cpc_rate is not null then 'CPC'
        when cpm_rate is not null then 'CPM'
        when cpa_percentage is not null then 'CPA'
        else null
    end as product_type,

    -- Validate CPC rate
    case
        when product_type = 'CPC' 
        and (cpc_rate is null or cpc_rate <= 0) 
        then null else cpc_rate 
    end as cpc_rate,

    -- Validate CPM rate
    case
        when product_type = 'CPM'
            and ( safe_cast(cpm_rate as float64) is null
            or safe_cast(cpm_rate as float64) <= 0) then null
            else safe_cast(cpm_rate as float64)
    end as cmp_rate,

    -- Validate CPA percentage
    case
        when
            product_type = 'CPA'
            and (cpa_percentage is null or cpa_percentage <= 0 or cpa_percentage > 1) then null
            else cpa_percentage
    end as cpa_percentage,

 
    coalesce( cs.standard_company_name, initcap(c.company_name), "Unknown") as company_name,
    coalesce(c.company_domain, "Unknown") as company_domain,

    created_at,
    valid_from,
    valid_to,
    current_record

from {{ source("raw_trendii", "dim_campaign_ext") }} c

left join
    {{ ref("company_standardization") }} cs
    on lower(c.company_domain) = lower(cs.company_domain)

where c.id is not null and c.brand_id is not null
