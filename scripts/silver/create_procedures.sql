
-- create silver.usp_load_orders procedure

CREATE OR ALTER PROCEDURE silver.usp_load_orders
AS
    BEGIN

        TRUNCATE TABLE silver.orders;

        INSERT INTO silver.orders (
            order_id,
            customer_id,
            order_status,
            order_purchase_timestamp,
            order_approved_at,
            order_delivered_carrier_date,
            order_delivered_customer_date,
            order_estimated_delivery_date
        )
        SELECT 
	        TRIM(REPLACE(order_id,'"','')) AS order_id,
            TRIM(REPLACE(customer_id,'"','')) AS customer_id,
            LOWER(TRIM(order_status)) AS order_status,
            TRY_CAST(order_purchase_timestamp AS DATETIME2) AS order_purchase_timestamp,
            TRY_CAST(order_approved_at AS DATETIME2) AS order_approved_at,
            TRY_CAST(order_delivered_carrier_date AS DATETIME2) AS order_delivered_carrier_date,
            TRY_CAST(order_delivered_customer_date AS DATETIME2) AS order_delivered_customer_date,
            TRY_CAST(order_estimated_delivery_date AS DATETIME2) AS order_estimated_delivery_date

        FROM bronze.orders;
    END;
GO







-- create silver.usp_load_order_items procedure


CREATE OR ALTER PROCEDURE silver.usp_load_order_items
AS
    BEGIN

        TRUNCATE TABLE silver.order_items;

        INSERT INTO silver.order_items (
            order_id,
            order_item_id,
            product_id,
            seller_id,
            shipping_limit_date,
            price,
            freight_value
        )
        SELECT 
	        TRIM(order_id) AS order_id,
            TRY_CAST(order_item_id AS INT) AS order_item_id,
            TRIM(product_id) AS product_id,
            TRIM(seller_id) AS seller_id,
            TRY_CAST(shipping_limit_date AS DATETIME2) AS shipping_limit_date,
            TRY_CAST(price AS DECIMAL(10,2)) AS price,
            TRY_CAST(freight_value AS DECIMAL(10,2)) AS freight_value
        FROM bronze.order_items;
    END;
GO

-- create silver.usp_load_customers procedure

CREATE OR ALTER PROCEDURE silver.usp_load_customers
AS
    BEGIN

        TRUNCATE TABLE silver.customers;

        INSERT INTO silver.customers (
            customer_id,
            customer_unique_id,
            customer_zip_code_prefix,
            customer_city,
            customer_state
        )
        SELECT 
            TRIM(REPLACE(customer_id,'"','')) AS customer_id,
            TRIM(REPLACE(customer_unique_id,'"','')) AS customer_unique_id,
            TRY_CAST(REPLACE(customer_zip_code_prefix,'"','') AS INT) AS customer_zip_code_prefix,
            UPPER(TRIM(customer_city)) AS customer_city,
            UPPER(TRIM(customer_state)) AS customer_state
        FROM bronze.customers;

    END;
GO


















