{{
    config(
        schema = 'staging_ecommerce'
    )
}}

WITH product_category_name_translation AS (

    SELECT *
    FROM
        {{ source(
            'ecommerce',
            'product_category_name_translation'
        ) }}
)

SELECT
    product_category_name_local,
    product_category_name_english
FROM
    product_category_name_translation
