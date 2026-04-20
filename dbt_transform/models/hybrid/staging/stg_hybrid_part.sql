-- STAGING HYBRID - TABLE: part
SELECT * FROM {{ source('raw_data', 'hybrid_part') }}