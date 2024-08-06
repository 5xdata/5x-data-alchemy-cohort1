{{
    config(
        schema = 'prod_ecommerce',
        materialized = 'table'
    )
}}

WITH products AS (
    SELECT 
        product_id,
        product_category_name,
        product_name_length,
        product_description_length,
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm, 
        product_width_cm
    FROM {{ ref('stg_ecommerce__products') }}
),

product_category_name_translation AS (
    SELECT 
        product_category_name_local,
        product_category_name_english
    FROM {{ ref('stg_ecommerce__product_category_name_translation') }}
)

SELECT 
    products.product_id,
    product_category_name_translation.product_category_name_english,
    products.product_category_name,
    products.product_name_length,
    products.product_description_length,
    products.product_photos_qty,
    products.product_weight_g,
    products.product_length_cm,
    products.product_height_cm,
    products.product_width_cm
FROM products
LEFT OUTER JOIN product_category_name_translation
ON product_category_name_translation.product_category_name_local = products.product_category_name
