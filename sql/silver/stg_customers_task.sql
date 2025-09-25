CREATE OR REPLACE TASK silver.process_stg_customers
    WAREHOUSE = 'COMPUTE_WH'
    SCHEDULE = '5 MINUTE'
AS
INSERT INTO silver.stg_customers (
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    address_line1,
    address_line2,
    city,
    state,
    postal_code,
    country,
    date_of_birth,
    registration_date,
    customer_status,
    customer_segment,
    created_at,
    updated_at
)
SELECT 
    payload:customer_id::VARCHAR AS customer_id,
    TRIM(UPPER(payload:first_name::VARCHAR)) AS first_name,
    TRIM(UPPER(payload:last_name::VARCHAR)) AS last_name,
    LOWER(TRIM(payload:email::VARCHAR)) AS email,
    REGEXP_REPLACE(payload:phone::VARCHAR, '[^0-9]', '') AS phone,
    TRIM(payload:address:line1::VARCHAR) AS address_line1,
    TRIM(payload:address:line2::VARCHAR) AS address_line2,
    TRIM(UPPER(payload:address:city::VARCHAR)) AS city,
    TRIM(UPPER(payload:address:state::VARCHAR)) AS state,
    TRIM(payload:address:postal_code::VARCHAR) AS postal_code,
    TRIM(UPPER(payload:address:country::VARCHAR)) AS country,
    TRY_TO_DATE(payload:date_of_birth::VARCHAR, 'YYYY-MM-DD') AS date_of_birth,
    TRY_TO_DATE(payload:registration_date::VARCHAR, 'YYYY-MM-DD') AS registration_date,
    COALESCE(UPPER(payload:status::VARCHAR), 'ACTIVE') AS customer_status,
    COALESCE(UPPER(payload:segment::VARCHAR), 'STANDARD') AS customer_segment,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at
FROM bronze.raw_customers_stream
WHERE METADATA$ACTION = 'INSERT'
  AND payload:customer_id IS NOT NULL
  AND payload:email IS NOT NULL;
