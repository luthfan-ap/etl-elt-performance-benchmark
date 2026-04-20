-- STAGING HYBRID - TABLE: nation
SELECT * FROM {{ source('raw_data', 'hybrid_nation') }}