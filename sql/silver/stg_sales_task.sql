CREATE OR REPLACE TASK silver.process_stg_sales
WAREHOUSE = 'your_warehouse_name'
SCHEDULE = '5 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('bronze.raw_sales_stream')
AS
INSERT INTO silver.stg_sales (order_id, customer_id, product_name, price, sale_date)
SELECT
    payload:order_id::VARCHAR,
    payload:customer_id::VARCHAR,
    payload:product_name::VARCHAR,
    payload:price::NUMBER(10, 2),
    payload:sale_date::DATE
FROM
    bronze.raw_sales_stream
WHERE
    METADATA$ACTION = 'INSERT'; -- Process only newly inserted rows
