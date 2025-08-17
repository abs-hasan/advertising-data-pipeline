{{ config(materialized="table",unique_key="publisher_id") }}

-- Build publisher dimension table
-- One row per publisher with domain, name, and when we first/last saw them

select

   
    publisher_id,
    publisher_domain,
    publisher_name,

    -- First and last time we saw this publisher in the data
    first_seen,
    last_seen

from {{ ref("int_publishers") }}

