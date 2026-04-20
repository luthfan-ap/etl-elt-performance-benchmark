-- STAGING HYBRID - TABLE: lineitem
SELECT * FROM {{ source('raw_data', 'hybrid_lineitem') }}