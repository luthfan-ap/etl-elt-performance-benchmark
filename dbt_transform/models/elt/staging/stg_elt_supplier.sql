-- STAGING ELT - TABLE: supplier
-- Cleaning: Type Casting
WITH source_data AS (
    SELECT * FROM {{ source('raw_data', 'elt_supplier') }}
),
casted_data AS (
    SELECT
        CAST(s_suppkey AS INTEGER) AS s_suppkey,
        CAST(s_nationkey AS INTEGER) AS s_nationkey

    FROM source_data
)
SELECT * FROM casted_data