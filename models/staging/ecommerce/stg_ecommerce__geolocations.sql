{{
    config(
        schema = 'staging_ecommerce'
    )
}}

WITH geolocation AS (

    SELECT *
    FROM
        {{ source(
            'ecommerce',
            'geolocation'
        ) }}
)

SELECT
	zip_code AS geolocation_zip_code,
	geolocation_lat,
	geolocationlng AS geolocation_lng,
	geolocationstate AS geolocation_state,
	city AS geolocation_city
FROM
    geolocation