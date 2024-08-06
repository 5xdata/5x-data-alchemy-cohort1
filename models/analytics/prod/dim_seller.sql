{{
    config(
        schema = 'prod_ecommerce',
        materialized = 'table'
    )
}}


WITH sellers AS (
    SELECT
        seller_id,
        seller_zip_code,
        seller_city,
        seller_state
    FROM  {{ ref('stg_ecommerce__sellers') }}
),

geolocation AS (
    SELECT 
        geolocation_zip_code,
        geolocation_lat,
        geolocation_lng
    FROM {{ ref('stg_ecommerce__geolocations') }}
)

SELECT sellers.seller_id,
    sellers.seller_zip_code,
    sellers.seller_city,
    sellers.seller_state,
    geolocation.geolocation_lat,
    geolocation.geolocation_lng
FROM sellers
LEFT OUTER JOIN geolocation
ON sellers.seller_zip_code = geolocation.geolocation_zip_code

