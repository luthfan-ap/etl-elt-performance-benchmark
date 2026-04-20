-- STAGING HYBRID - TABLE: orders
SELECT * FROM {{ source('raw_data', 'hybrid_orders') }}