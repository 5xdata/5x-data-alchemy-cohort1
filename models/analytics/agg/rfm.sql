{{
    config(
        schema = 'analytics_ecommerce',
        materialized = 'table'
    )
}}

WITH RecencyScore AS (
    SELECT uuid,
           MAX(order_purchase_timestamp) AS last_purchase,
           NTILE(5) OVER (ORDER BY MAX(order_purchase_timestamp) DESC) AS recency
    FROM {{ ref('stg_ecommerce__orders') }} orders
    JOIN {{ ref('stg_ecommerce__customers') }} customers on orders.customer_id = customers.customer_id
    WHERE order_status = 'delivered'
    GROUP BY uuid
),

FrequencyScore AS (
    SELECT uuid,
           COUNT(order_id) AS total_orders,
           NTILE(5) OVER (ORDER BY COUNT(order_id) DESC) AS frequency
    FROM {{ ref('stg_ecommerce__orders') }} orders
    JOIN {{ ref('stg_ecommerce__customers') }} customers on orders.customer_id = customers.customer_id
    WHERE order_status = 'delivered'
    GROUP BY uuid
),

MonetaryScore AS (
    SELECT uuid,
           SUM(ORDER_VALUE) AS total_spent,
           NTILE(5) OVER (ORDER BY SUM(price) DESC) AS monetary

    FROM {{ ref('stg_ecommerce__orders') }} orders
    JOIN {{ ref('stg_ecommerce__order_items') }} order_items on order_items.order_id = orders.order_id
    JOIN {{ ref('stg_ecommerce__customers') }} customers on orders.customer_id = customers.customer_id
    WHERE order_status = 'delivered'
    GROUP BY uuid
),

final AS (
    SELECT 
        RecencyScore.uuid,
        last_purchase,
        total_orders,
        total_spent,
        CASE
            WHEN recency = 1 AND (frequency + monetary) IN (1, 2, 3, 4) THEN 'Champions'
            WHEN recency IN (4, 5) AND (frequency + monetary) IN (1, 2) THEN 'Can''t Lose Them'
            WHEN recency IN (4, 5) AND (frequency + monetary) IN (3, 4, 5, 6) THEN 'Hibernating'
            WHEN recency IN (4, 5) AND (frequency + monetary) IN (7, 8, 9, 10) THEN 'Lost'
            WHEN recency IN (2, 3) AND (frequency + monetary) IN (1, 2, 3, 4) THEN 'Loyal Customers'
            WHEN recency = 3 AND (frequency + monetary) IN (5, 6) THEN 'Needs Attention'
            WHEN recency = 1 AND (frequency + monetary) IN (7, 8) THEN 'Recent Users'
            WHEN (recency = 1 AND (frequency + monetary) IN (5, 6)) OR
                 (recency = 2 AND (frequency + monetary) IN (5, 6, 7, 8)) THEN 'Potential Loyalists'
            WHEN recency = 1 AND (frequency + monetary) IN (9, 10) THEN 'Price Sensitive'
            WHEN recency = 2 AND (frequency + monetary) IN (9, 10) THEN 'Promising'
            WHEN recency = 3 AND (frequency + monetary) IN (7, 8, 9, 10) THEN 'About to Sleep'
        END AS RFM_Bucket
    FROM RecencyScore
    JOIN FrequencyScore on RecencyScore.uuid = FrequencyScore.uuid
    JOIN MonetaryScore on RecencyScore.uuid = MonetaryScore.uuid
)

SELECT * from final
