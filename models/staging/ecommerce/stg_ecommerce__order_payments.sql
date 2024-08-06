{{
    config(
        schema = 'staging_ecommerce'
    )
}}

WITH order_payments AS (

    SELECT *
    FROM
        {{ source(
            'ecommerce',
            'order_payments'
        ) }}
)

SELECT
    order_id,
    payment_sequential,
    CASE
        WHEN paymenttype IS NULL THEN 'not_defined'
        WHEN paymenttype = 'debitcard' THEN 'debit_card'
        ELSE paymenttype END AS payment_type,
    installments AS number_of_investments
FROM
    order_payments
