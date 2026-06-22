-- STAGING HYBRID - TABLE: nation
select
    cast(n_nationkey as integer) as n_nationkey,
    cast(n_name as text) as n_name
from {{ source('raw_data', 'hybrid_nation') }}