select
    nation,
    o_year,
    sum(amount) as sum_profit
from
    (
        select
            n.n_name as nation,
            extract(year from o.o_orderdate) as o_year,
            l.l_extendedprice * (1 - l.l_discount) - ps.ps_supplycost * l.l_quantity as amount
        from
            {{ ref('stg_elt_part') }} p,
            {{ ref('stg_elt_supplier') }} s,
            {{ ref('stg_elt_lineitem') }} l,
            {{ ref('stg_elt_partsupp') }} ps,
            {{ ref('stg_elt_orders') }} o,
            {{ ref('stg_elt_nation') }} n
        where
            s.s_suppkey = l.l_suppkey
            and ps.ps_suppkey = l.l_suppkey
            and ps.ps_partkey = l.l_partkey
            and p.p_partkey = l.l_partkey
            and o.o_orderkey = l.l_orderkey
            and s.s_nationkey = n.n_nationkey
            and p.p_name like '%green%'
    ) as profit
group by
    nation,
    o_year
order by
    nation asc,
    o_year desc