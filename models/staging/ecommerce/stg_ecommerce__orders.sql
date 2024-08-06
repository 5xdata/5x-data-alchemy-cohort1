{{
    config(
        schema = 'staging_ecommerce'
    )
}}

WITH orders AS (

    SELECT *
    FROM
        {{ source(
            'ecommerce',
            'orders'
        ) }}
)

SELECT
    id AS order_id,
	customer_id,
	status AS order_status,
	purchase_timestamp AS order_purchase_timestamp,
	order_approved_at,
	order_delivered_carrier_date,
	delivered_customer_date AS order_delivered_customer_date,
    DATE(estimated_delivery_date) AS order_estimated_delivery_date,
FROM
    orders
