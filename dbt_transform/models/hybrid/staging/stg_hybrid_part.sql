-- STAGING HYBRID - TABLE: part
select
    cast(p_partkey as integer) as p_partkey,
    cast(p_name as text) as p_name
from {{ source('raw_data', 'hybrid_part') }}