-- STAGING ELT - TABLE: partsupp
-- Cleaning: Type Casting
WITH source_data AS (
    SELECT * FROM {{ source('raw_data', 'elt_partsupp') }}
),
casted_data AS (
    SELECT
        CAST(ps_partkey AS INTEGER) AS ps_partkey,
        CAST(ps_suppkey AS INTEGER) AS ps_suppkey,
        CAST(ps_supplycost AS DOUBLE PRECISION) AS ps_supplycost

    FROM source_data
)
SELECT * FROM casted_data