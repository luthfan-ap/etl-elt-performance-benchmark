-- STAGING HYBRID - TABLE: supplier
select
    cast(s_suppkey as integer) as s_suppkey,
    cast(s_nationkey as integer) as s_nationkey
from {{ source('raw_data', 'hybrid_supplier') }}