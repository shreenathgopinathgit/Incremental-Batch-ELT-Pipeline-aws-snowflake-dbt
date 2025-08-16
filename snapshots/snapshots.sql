{% snapshot olist_orders_snapshot %}

{{ config(
    target_schema='snapshots',
    unique_key='order_id',
    strategy='check',
    check_cols=['order_status', 'order_approved_at', 'order_delivered_customer_date']
) }}

SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp AS order_purchase_ts,
    order_approved_at AS order_approved_ts,
    order_delivered_carrier_date AS carrier_pickup_ts,
    order_delivered_customer_date AS customer_delivery_ts,
    order_estimated_delivery_date AS estimated_delivery_date
FROM {{ source('movielens', 'olist_orders') }}
WHERE order_id IS NOT NULL

{% endsnapshot %}
