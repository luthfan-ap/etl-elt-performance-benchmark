-- STAGING HYBRID - TABLE: orders
select
    cast(o_orderkey as integer) as o_orderkey,
    cast(o_orderdate as date) as o_orderdate
from {{ source('raw_data', 'hybrid_orders') }}