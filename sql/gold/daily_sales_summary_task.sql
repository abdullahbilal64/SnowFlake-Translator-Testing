CREATE OR REPLACE TASK gold.process_daily_sales_summary
WAREHOUSE = 'your_warehouse_name'
AFTER silver.process_stg_sales -- Define dependency on the silver task
AS
MERGE INTO gold.daily_sales_summary AS target
USING (
    SELECT
        sale_date,
        SUM(price) AS total_revenue,
        COUNT(order_id) AS total_orders
    FROM
        silver.stg_sales
    JOIN
        silver.stg_customers
    ON
        silver.stg_sales.customer_id = silver.stg_customers.customer_id
    WHERE
        sale_date = CURRENT_DATE() -- Assuming daily run
        AND silver.stg_customers.customer_status = 'ACTIVE'
    GROUP BY
        sale_date
) AS source ON target.sale_date = source.sale_date
WHEN MATCHED THEN UPDATE SET
    target.total_revenue = source.total_revenue,
    target.total_orders = source.total_orders
WHEN NOT MATCHED THEN INSERT
    (sale_date, total_revenue, total_orders)
VALUES
    (source.sale_date, source.total_revenue, source.total_orders);

-- A separate root task is needed to trigger the task tree.
-- For a schedule, `silver.process_stg_sales` would be defined with a schedule.
-- The gold task would then run after the silver task completes.
