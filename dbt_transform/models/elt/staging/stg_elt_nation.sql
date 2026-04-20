-- STAGING ELT - TABLE: nation
-- Cleaning: Type Casting
WITH source_data AS (
    SELECT * FROM {{ source('raw_data', 'elt_nation') }}
),
casted_data AS (
    SELECT
        CAST(n_nationkey AS INTEGER) AS n_nationkey,
        CAST(n_name AS VARCHAR) AS n_name

    FROM source_data
)
SELECT * FROM casted_data