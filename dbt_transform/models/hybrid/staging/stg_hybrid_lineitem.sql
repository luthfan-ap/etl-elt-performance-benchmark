-- STAGING HYBRID - TABLE: lineitem
select
    cast(l_orderkey as integer) as l_orderkey,
    cast(l_partkey as integer) as l_partkey,
    cast(l_suppkey as integer) as l_suppkey,
    cast(l_quantity as double precision) as l_quantity,
    cast(l_extendedprice as double precision) as l_extendedprice,
    cast(l_discount as double precision) as l_discount
from {{ source('raw_data', 'hybrid_lineitem') }}