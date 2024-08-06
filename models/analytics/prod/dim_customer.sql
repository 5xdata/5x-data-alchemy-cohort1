{{
    config(
        schema = 'prod_ecommerce',
        materialized = 'table'
    )
}}

WITH customers AS (
    SELECT 
        customer_id,
        uuid,
        customer_zip_code,
        customer_city,
        customer_state
    FROM {{ ref('stg_ecommerce__customers') }}
),

geolocation AS (
    SELECT 
        geolocation_zip_code,
        geolocation_lat,
        geolocation_lng
    FROM {{ ref('stg_ecommerce__geolocations') }}
)

SELECT 
    customers.customer_id,
    customers.uuid,
    customers.customer_zip_code,
    customers.customer_city,
    customers.customer_state,
    geolocation.geolocation_lat,
    geolocation.geolocation_lng
FROM customers
LEFT OUTER JOIN geolocation
ON customers.customer_zip_code = geolocation.geolocation_zip_code
