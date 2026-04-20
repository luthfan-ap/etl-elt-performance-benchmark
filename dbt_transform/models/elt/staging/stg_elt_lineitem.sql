-- STAGING ELT - TABLE: lineitem
-- Cleaning: Type Casting
WITH source_data AS (
    SELECT * FROM  {{ source('raw_data', 'elt_lineitem') }}
),

casted_data AS (
    SELECT
        CAST(l_orderkey AS INTEGER) AS l_orderkey,
        CAST(l_partkey AS INTEGER) AS l_partkey,
        CAST(l_suppkey AS INTEGER) AS l_suppkey,
        CAST(l_quantity AS DOUBLE PRECISION) AS l_quantity,
        CAST(l_extendedprice AS DOUBLE PRECISION) AS l_extendedprice,
        CAST(l_discount AS DOUBLE PRECISION) AS l_discount
        
        -- Kita hanya mengambil 6 kolom yang dibutuhkan untuk Query 9 (Column Pruning)
        -- 10 kolom lainnya otomatis terbuang/ditinggalkan
    FROM source_data
)

SELECT * FROM casted_data