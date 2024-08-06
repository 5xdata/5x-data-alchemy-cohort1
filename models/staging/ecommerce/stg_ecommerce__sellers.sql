{{
    config(
        schema = 'staging_ecommerce'
    )
}}

WITH sellers AS (

    SELECT *
    FROM
        {{ source(
            'ecommerce',
            'sellers'
        ) }}
)

SELECT
	id AS seller_id,
	zip_code AS seller_zip_code,
	city AS seller_city,
	state AS seller_state
FROM
    sellers
