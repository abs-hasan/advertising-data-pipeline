-- snapshots/snap_dim_campaigns.sql
{% snapshot snap_dim_campaigns %}
    {{
        config(
          target_schema='snapshots',
          unique_key='campaign_id',
          strategy='check',
          check_cols=['campaign_name', 'company_name'],
        )
    }}
    
    SELECT 
        campaign_id,
        campaign_name,
        company_name,
        brand_id
    FROM {{ source('raw_trendii', 'dim_campaign_ext') }}
    
{% endsnapshot %}
