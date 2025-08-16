{{ config(
    materialized = "incremental",
    unique_key = "customer_id"
) }}

WITH customers AS (
    SELECT DISTINCT
        customer_id
    FROM {{ ref('src_orders') }}
)

SELECT *
FROM customers

{% if is_incremental() %}
-- Only insert new customers in incremental mode
WHERE customer_id NOT IN (SELECT customer_id FROM {{ this }})
{% endif %}
