{{ config(
    materialized = "incremental",
    unique_key = "order_id"
) }}

WITH src AS (
    SELECT *
    FROM {{ source('movielens', 'olist_orders') }}
    WHERE order_id IS NOT NULL
)

SELECT
    CAST(order_id AS STRING) AS order_id,
    CAST(customer_id AS STRING) AS customer_id,
    CAST(order_status AS STRING) AS order_status,
    CAST(order_purchase_timestamp AS TIMESTAMP) AS order_purchase_ts,
    CAST(order_approved_at AS TIMESTAMP) AS order_approved_ts,
    CAST(order_delivered_carrier_date AS TIMESTAMP) AS carrier_pickup_ts,
    CAST(order_delivered_customer_date AS TIMESTAMP) AS customer_delivery_ts,
    CAST(order_estimated_delivery_date AS DATE) AS estimated_delivery_date
FROM src

{% if is_incremental() %}
-- Only include rows that are new or updated since the last run
WHERE order_purchase_timestamp > (
    SELECT COALESCE(MAX(order_purchase_ts), '1900-01-01'::TIMESTAMP)
    FROM {{ this }}
)
{% endif %}
