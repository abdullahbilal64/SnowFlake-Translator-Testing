CREATE OR REPLACE TABLE bronze.raw_customers (
    id UUID,
    payload VARIANT,
    load_ts TIMESTAMP_LTZ
);
