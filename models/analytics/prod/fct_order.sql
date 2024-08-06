{{
    config(
        schema = 'prod_ecommerce',
        materialized = 'table'
    )
}}

WITH orders AS (
    SELECT 
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date
    FROM {{ ref('stg_ecommerce__orders') }}
),

order_items AS (
    SELECT 
        order_id,
        product_id,
        seller_id,
        shipping_limit_date,
        sum(coalesce(price, 0)) as total_price,
        sum(coalesce(delivery_charges, 0)) as total_delivery_Charges,
        sum(coalesce(order_value, 0)) as total_order_value
    FROM {{ ref('stg_ecommerce__order_items') }}
    GROUP BY ALL
),

order_payments AS (
    SELECT 
        order_id,
        payment_sequential,
        payment_type,
        number_of_investments
    FROM {{ ref('stg_ecommerce__order_payments') }}
),

order_reviews AS (
    SELECT 
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date,
        review_answer_timestamp
    FROM {{ ref('stg_ecommerce__order_reviews') }}
),

final AS (
    SELECT DISTINCT
        orders.order_id,
        orders.customer_id,
        orders.order_status,
        orders.order_purchase_timestamp,
        orders.order_approved_at,
        orders.order_delivered_carrier_date,
        orders.order_delivered_customer_date,
        orders.order_estimated_delivery_date,
        order_items.product_id,
        order_items.seller_id,
        order_items.shipping_limit_date,
        order_items.total_price,
        order_items.total_delivery_Charges,
        order_items.total_order_value,
        order_payments.payment_sequential,
        order_payments.number_of_investments,
        order_payments.payment_type,
        order_reviews.review_id,
        order_reviews.review_score,
        order_reviews.review_comment_title,
        order_reviews.review_comment_message,
        order_reviews.review_creation_date,
        order_reviews.review_answer_timestamp 
    FROM orders
    LEFT OUTER JOIN order_items ON orders.order_id = order_items.order_id
    LEFT OUTER JOIN order_payments ON orders.order_id = order_payments.order_id
    LEFT OUTER JOIN order_reviews ON order_reviews.order_id = orders.order_id
)

SELECT * FROM final