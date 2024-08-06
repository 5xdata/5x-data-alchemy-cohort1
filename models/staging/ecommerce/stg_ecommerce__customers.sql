{{
    config(
        schema = 'staging_ecommerce'
    )
}}

WITH customers AS (

    SELECT *
    FROM {{ source(
            'ecommerce',
            'customers'
        ) }}
)

SELECT
    id AS customer_id,
    uuid,
    zip_code AS customer_zip_code,
    customer_city,
    state AS customer_state
FROM
    customers