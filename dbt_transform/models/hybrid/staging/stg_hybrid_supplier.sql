-- STAGING HYBRID - TABLE: supplier
SELECT * FROM {{ source('raw_data', 'hybrid_supplier') }}