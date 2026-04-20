-- STAGING HYBRID - TABLE: partsupp
SELECT * FROM {{ source('raw_data', 'hybrid_partsupp') }}