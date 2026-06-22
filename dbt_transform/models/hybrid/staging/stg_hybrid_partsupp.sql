-- STAGING HYBRID - TABLE: partsupp
select
    cast(ps_partkey as integer) as ps_partkey,
    cast(ps_suppkey as integer) as ps_suppkey,
    cast(ps_supplycost as double precision) as ps_supplycost
from {{ source('raw_data', 'hybrid_partsupp') }}