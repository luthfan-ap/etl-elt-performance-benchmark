CREATE SCHEMA IF NOT EXISTS raw_layer;
DROP TABLE IF EXISTS raw_layer.nation;
CREATE TABLE raw_layer.nation (
    n_nationkey integer,
    n_name varchar(25),
    n_regionkey integer,
    n_comment varchar(152)
);
DROP TABLE IF EXISTS raw_layer.region;
CREATE TABLE raw_layer.region (
    r_regionkey integer,
    r_name varchar(25),
    r_comment varchar(152)
);
DROP TABLE IF EXISTS raw_layer.part;
CREATE TABLE raw_layer.part (
    p_partkey integer,
    p_name varchar(55),
    p_mfgr varchar(25),
    p_brand varchar(10),
    p_type varchar(25),
    p_size integer,
    p_container varchar(10),
    p_retailprice decimal(12, 2),
    p_comment varchar(231)
);
DROP TABLE IF EXISTS raw_layer.supplier;
CREATE TABLE raw_layer.supplier (
    s_suppkey integer,
    s_name varchar(25),
    s_address varchar(40),
    s_nationkey integer,
    s_phone varchar(15),
    s_acctbal decimal(12, 2),
    s_comment varchar(101)
);
DROP TABLE IF EXISTS raw_layer.partsupp;
CREATE TABLE raw_layer.partsupp (
    ps_partkey integer,
    ps_suppkey integer,
    ps_availqty integer,
    ps_supplycost decimal(12, 2),
    ps_comment varchar(199)
);
DROP TABLE IF EXISTS raw_layer.customer;
CREATE TABLE raw_layer.customer (
    c_custkey integer,
    c_name varchar(25),
    c_address varchar(40),
    c_nationkey integer,
    c_phone varchar(15),
    c_acctbal decimal(12, 2),
    c_mktsegment varchar(10),
    c_comment varchar(117)
);
DROP TABLE IF EXISTS raw_layer.orders;
CREATE TABLE raw_layer.orders (
    o_orderkey integer,
    o_custkey integer,
    o_orderstatus varchar(1),
    o_totalprice decimal(12, 2),
    o_orderdate date,
    o_orderpriority varchar(15),
    o_clerk varchar(15),
    o_shippriority integer,
    o_comment varchar(79)
);
DROP TABLE IF EXISTS raw_layer.lineitem;
CREATE TABLE raw_layer.lineitem (
    l_orderkey integer,
    l_partkey integer,
    l_suppkey integer,
    l_linenumber integer,
    l_quantity integer,
    l_extendedprice decimal(12, 2),
    l_discount decimal(12, 2),
    l_tax decimal(12, 2),
    l_returnflag varchar(1),
    l_linestatus varchar(1),
    l_shipdate date,
    l_commitdate date,
    l_receiptdate date,
    l_shipinstruct varchar(25),
    l_shipmode varchar(10),
    l_comment varchar(44)
);