{{
    config(
        schema = 'analytics_ecommerce',
        materialized = 'table'
    )
}}

WITH orders AS (
    SELECT 
        ORDER_ID,
        SUM(COALESCE(total_order_value, 0)) as order_value,
        order_status,
        order_purchase_timestamp,
        review_score,
        CUSTOMER_ID,
        COUNT(*) AS TOTAL_ORDERS
    FROM {{ ref('fct_order') }}
    GROUP BY ALL
),

CUSTOMERS AS (
    SELECT customer_id,
        uuid
    from {{ ref('dim_customer') }}
),

order_diff AS (
    SELECT uuid,
        ORDER_ID,
        order_purchase_timestamp,
        LAG(order_purchase_timestamp) OVER (PARTITION BY uuid ORDER BY order_purchase_timestamp) as last_purchase,
        COALESCE(DATEDIFF("DAY",  last_purchase, order_purchase_timestamp), 0) as date_diff
    FROM CUSTOMERS 
    JOIN orders ON CUSTOMERS.CUSTOMER_ID = orders.CUSTOMER_ID
)

SELECT 
    CUSTOMERS.uuid,
    count(*) AS total_orders,
    SUM(order_value) AS total_spent,
    CASE 
        WHEN COUNT(*) > 1 THEN 'repeated customer'
        ELSE 'one-time'
    END AS customer_type,
    SUM(CASE WHEN order_status = 'delivered' THEN 1 ELSE 0 END) as total_delivered,
    AVG(orders.order_value) AS average_order_value,
    MIN(orders.order_purchase_timestamp) AS first_order,
    MAX(orders.order_purchase_timestamp) AS most_recent_order,
    AVG(orders.review_score) AS average_review_score,
    AVG(order_diff.date_diff) AS avg_days_bw_orders
FROM CUSTOMERS 
LEFT JOIN orders ON CUSTOMERS.CUSTOMER_ID = orders.CUSTOMER_ID
LEFT JOIN order_diff ON CUSTOMERS.uuid = order_diff.uuid
group by all