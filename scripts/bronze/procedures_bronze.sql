-- create silver.orders procedure

CREATE OR ALTER PROCEDURE silver.usp_load_orders
AS
    BEGIN
        SET NOCOUNT ON;

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
	        TRIM(order_id) AS order_id,
            TRIM(customer_id) AS customer_id,
            LOWER(TRIM(order_status)) AS order_status,
            TRY_CAST(order_purchase_timestamp AS DATETIME2) AS order_purchase_timestamp,
            TRY_CAST(order_approved_at AS DATETIME2) AS order_approved_at,
            TRY_CAST(order_delivered_carrier_date AS DATETIME2) AS order_delivered_carrier_date,
            TRY_CAST(order_delivered_customer_date AS DATETIME2) AS order_delivered_customer_date,
            TRY_CAST(order_estimated_delivery_date AS DATETIME2) AS order_estimated_delivery_date

        FROM bronze.orders;
    END;
GO
