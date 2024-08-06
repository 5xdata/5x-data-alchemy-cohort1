{{
    config(
        schema = 'staging_ecommerce'
    )
}}

WITH products AS (

    SELECT *
    FROM
        {{ source(
            'ecommerce',
            'products'
        ) }}
)

SELECT
	id AS product_id,
	category_name AS product_category_name,
	name_length AS product_name_length,
	productdescription_length AS product_description_length,
	photos_qty AS product_photos_qty,
	weight_g AS product_weight_g,
	length_cm AS product_length_cm,
	height_cm AS product_height_cm, 
	width_cm AS product_width_cm
FROM
    products
