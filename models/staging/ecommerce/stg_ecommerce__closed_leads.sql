{{
    config(
        schema = 'staging_ecommerce'
    )
}}

WITH leads_closed AS (

    SELECT *
    FROM
        {{ source(
            'marketing_funnel',
            'leads_closed'
        ) }}
)

SELECT
    mql_id,
	average_stock,
	seller_id,
	business_type,
	won_date,
	declared_product_catalog_size,
	sr_id,
	declared_monthly_revenue,
	sdr_id,
	business_segment,
	lead_type,
	lead_behaviour_profile,
	has_company,
	has_gtin
FROM
    leads_closed