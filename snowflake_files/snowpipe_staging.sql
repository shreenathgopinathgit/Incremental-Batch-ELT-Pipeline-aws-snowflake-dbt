create or replace stage olist_orders_stage
storage_integration = s3_int_olist_orders
url = 's3://movielensdbtbucket/ecom/raw/olist_orders/'
FILE_FORMAT = (
    TYPE = 'CSV'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
);

CREATE TABLE olist_orders (
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    order_status VARCHAR(50),
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);

CREATE OR REPLACE PIPE olist_orders_pipe
  AUTO_INGEST = TRUE
  AS
  COPY INTO olist_orders
  FROM @olist_orders_stage
  FILE_FORMAT = (
    TYPE = 'CSV'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
);

  
CREATE OR REPLACE STORAGE INTEGRATION s3_int_olist_orders
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::8997233:role/s3ingeststowspipe'
  STORAGE_ALLOWED_LOCATIONS = ('s3://movielensdbtbucket/ecom/raw/olist_orders/');

DESC INTEGRATION s3_int_olist_orders;

select system$pipe_status('olist_orders_pipe');
