{{
    config(
        schema = 'staging_ecommerce'
    )
}}

WITH leads_qualified AS (

    SELECT *
    FROM
        {{ source(
            'marketing_funnel',
            'leads_qualified'
        ) }}
)

SELECT
    mql_id,
	first_contact_date,
	landing_page_id,
	origin
FROM
    leads_qualified