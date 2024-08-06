{{
    config(
        schema = 'staging_ecommerce'
    )
}}

WITH order_items AS (

    SELECT *
    FROM
        {{ source(
            'ecommerce',
            'order_items'
        ) }}
)

SELECT order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    delivery_charges,
    coalesce(price, 0) + coalesce(delivery_charges, 0) AS order_value
FROM order_items