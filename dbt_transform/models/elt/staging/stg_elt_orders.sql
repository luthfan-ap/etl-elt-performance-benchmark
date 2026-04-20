-- STAGING ELT - TABLE: orders
-- Cleaning: Type Casting
WITH source_data AS (
    SELECT * FROM {{ source('raw_data', 'elt_orders') }}
),
casted_data AS (
    SELECT
        CAST(o_orderkey AS INTEGER) AS o_orderkey,
        CAST(o_orderdate AS DATE) AS o_orderdate
    FROM source_data
)
SELECT * FROM casted_data