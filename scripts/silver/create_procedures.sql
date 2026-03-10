
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
