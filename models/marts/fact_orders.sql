 {{ config(
    materialized = "incremental",
    unique_key = "order_id"
) }}

WITH orders AS (
    SELECT *
    FROM {{ ref('src_orders') }}
)

SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_ts,
    order_approved_ts,
    carrier_pickup_ts,
    customer_delivery_ts,
    estimated_delivery_date,
    DATEDIFF('day', order_purchase_ts, order_approved_ts) AS days_to_approve,
    DATEDIFF('day', order_approved_ts, carrier_pickup_ts) AS days_to_ship,
    DATEDIFF('day', carrier_pickup_ts, customer_delivery_ts) AS days_to_deliver,
    DATEDIFF('day', order_purchase_ts, customer_delivery_ts) AS total_delivery_days
FROM orders

{% if is_incremental() %}
WHERE order_purchase_ts > (
    SELECT COALESCE(MAX(order_purchase_ts), '1900-01-01'::TIMESTAMP)
    FROM {{ this }}
)
{% endif %}
