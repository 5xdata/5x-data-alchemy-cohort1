{{
    config(
        schema = 'staging_ecommerce'
    )
}}

WITH order_reviews AS (

    SELECT *
    FROM
        {{ source(
            'ecommerce',
            'order_reviews'
        ) }}
)

SELECT
    review_id,
	order_id,
	review_score,
	review_comment_title,
	review_comment_message,
	review_creation_date,
	review_answer_timestamp
FROM
    order_reviews
