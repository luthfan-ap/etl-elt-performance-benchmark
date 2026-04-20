-- STAGING ELT - TABLE: part
-- Cleaning: Type Casting
WITH source_data AS (
    SELECT * FROM {{ source('raw_data', 'elt_part') }}
),
casted_data AS (
    SELECT
        CAST(p_partkey AS INTEGER) AS p_partkey,
        CAST(p_name AS VARCHAR) AS p_name
    FROM source_data
)
SELECT * FROM casted_data