CREATE OR REPLACE TABLE silver.stg_customers (
    customer_id VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    email VARCHAR,
    phone VARCHAR,
    address_line1 VARCHAR,
    address_line2 VARCHAR,
    city VARCHAR,
    state VARCHAR,
    postal_code VARCHAR,
    country VARCHAR,
    date_of_birth DATE,
    registration_date DATE,
    customer_status VARCHAR,
    customer_segment VARCHAR,
    created_at TIMESTAMP_LTZ,
    updated_at TIMESTAMP_LTZ
);
