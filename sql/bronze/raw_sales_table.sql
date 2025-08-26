CREATE OR REPLACE TABLE bronze.raw_sales (
    id UUID,
    payload VARIANT,
    load_ts TIMESTAMP_LTZ
);
