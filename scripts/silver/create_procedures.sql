
-- 1. create silver.usp_load_orders procedure

CREATE OR ALTER PROCEDURE silver.usp_load_orders
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @row_count INT;

    BEGIN TRY

        -- START LOG
        INSERT INTO etl.etl_logs (
            process_name, layer, status
        )
        VALUES (
            'usp_load_orders', 'Silver', 'START'
        );

        -- MAIN LOGIC

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

        SET @row_count = @@ROWCOUNT;

        -- SUCCESS LOG
        INSERT INTO etl.etl_logs (
            process_name, layer, status, rows_processed
        )
        VALUES (
            'usp_load_orders', 'Silver', 'SUCCESS', @row_count
        );

    END TRY

    BEGIN CATCH

        -- ERROR LOG
        INSERT INTO etl.etl_logs (
            process_name, layer, status, error_message
        )
        VALUES (
            'usp_load_orders', 'Silver', 'FAILED', ERROR_MESSAGE()
        );

    END CATCH
END;






-- 2.create silver.usp_load_order_items procedure


CREATE OR ALTER PROCEDURE silver.usp_load_order_items
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @row_count INT;

    BEGIN TRY

        -- START LOG
        INSERT INTO etl.etl_logs (
            process_name, layer, status
        )
        VALUES (
            'usp_load_order_items', 'Silver', 'START'
        );

        -- MAIN LOGIC

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
	        TRIM(REPLACE(order_id,'"','')) AS order_id,
            TRY_CAST(order_item_id AS INT) AS order_item_id,
            TRIM(REPLACE(product_id,'"','')) AS product_id,
            TRIM(REPLACE(seller_id,'"','')) AS seller_id,
            TRY_CAST(shipping_limit_date AS DATETIME2) AS shipping_limit_date,
            TRY_CAST(price AS DECIMAL(10,2)) AS price,
            TRY_CAST(freight_value AS DECIMAL(10,2)) AS freight_value
        FROM bronze.order_items;

        SET @row_count = @@ROWCOUNT;

        -- SUCCESS LOG
        INSERT INTO etl.etl_logs (
            process_name, layer, status, rows_processed
        )
        VALUES (
            'usp_load_order_items', 'Silver', 'SUCCESS', @row_count
        );

    END TRY

    BEGIN CATCH

        -- ERROR LOG
        INSERT INTO etl.etl_logs (
            process_name, layer, status, error_message
        )
        VALUES (
            'usp_load_order_items', 'Silver', 'FAILED', ERROR_MESSAGE()
        );

    END CATCH
END;     





            
-- 3. create silver.usp_load_customers procedure

CREATE OR ALTER PROCEDURE silver.usp_load_customers
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @row_count INT;

    BEGIN TRY

        -- 🔹 START LOG
        INSERT INTO etl.etl_logs (
            process_name, layer, status
        )
        VALUES (
            'usp_load_customers', 'Silver', 'START'
        );

        -- 🔹 MAIN LOGIC
        TRUNCATE TABLE silver.customers;

        INSERT INTO silver.customers (
            customer_id,
            customer_unique_id,
            customer_zip_code_prefix,
            customer_city,
            customer_state
        )
        SELECT 
            TRIM(REPLACE(customer_id,'"','')),
            TRIM(REPLACE(customer_unique_id,'"','')),
            TRY_CAST(REPLACE(customer_zip_code_prefix,'"','') AS INT),
            UPPER(TRIM(customer_city)),
            UPPER(TRIM(customer_state))
        FROM bronze.customers
        WHERE customer_id IS NOT NULL;

        SET @row_count = @@ROWCOUNT;

        -- 🔹 SUCCESS LOG
        INSERT INTO etl.etl_logs (
            process_name, layer, status, rows_processed
        )
        VALUES (
            'usp_load_customers', 'Silver', 'SUCCESS', @row_count
        );

    END TRY
    BEGIN CATCH

        -- 🔹 ERROR LOG
        INSERT INTO etl.etl_logs (
            process_name, layer, status, error_message
        )
        VALUES (
            'usp_load_customers', 'Silver', 'FAILED', ERROR_MESSAGE()
        );

        THROW;

    END CATCH
END;
GO
	


-- 4. create  silver.usp_load_sellers procedure

CREATE OR ALTER PROCEDURE silver.usp_load_sellers
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @row_count INT;

    BEGIN TRY

        -- 🔹 START LOG
        INSERT INTO etl.etl_logs (
            process_name, layer, status
        )
        VALUES (
            'usp_load_sellers', 'Silver', 'START'
        );

        -- 🔹 MAIN LOGIC
        TRUNCATE TABLE silver.sellers;

        INSERT INTO silver.sellers (
            seller_id,
            seller_zip_code_prefix,
            seller_city,
            seller_state
        )
        SELECT 
            TRIM(REPLACE(seller_id,'"','')),
            TRY_CAST(TRIM(REPLACE(seller_zip_code_prefix,'"','')) AS INT),
            UPPER(TRIM(seller_city)),
            UPPER(RIGHT(TRIM(REPLACE(seller_state,'"','')),2))
        FROM bronze.sellers
        WHERE seller_id IS NOT NULL;

        SET @row_count = @@ROWCOUNT;

        -- 🔹 SUCCESS LOG
        INSERT INTO etl.etl_logs (
            process_name, layer, status, rows_processed
        )
        VALUES (
            'usp_load_sellers', 'Silver', 'SUCCESS', @row_count
        );

    END TRY
    BEGIN CATCH

        -- 🔹 ERROR LOG
        INSERT INTO etl.etl_logs (
            process_name, layer, status, error_message
        )
        VALUES (
            'usp_load_sellers', 'Silver', 'FAILED', ERROR_MESSAGE()
        );

        THROW;

    END CATCH
END;
GO






-- 5. create silver.usp_load_products procedure

CREATE OR ALTER PROCEDURE silver.usp_load_products
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @row_count INT;

    BEGIN TRY

        -- START LOG
        INSERT INTO etl.etl_logs (process_name, layer, status)
        VALUES ('usp_load_products', 'Silver', 'START');

        -- MAIN LOGIC
        TRUNCATE TABLE silver.products;

        WITH cleaned_data AS (
            SELECT
                REPLACE(TRIM(p.product_id), '"', '') AS product_id,
                TRIM(p.product_category_name) AS product_category_name,
                TRIM(t.product_category_name_english) AS product_category_name_english,
                TRIM(p.product_name_lenght) AS product_name_length,
                TRIM(p.product_description_lenght) AS product_description_length,
                TRIM(p.product_photos_qty) AS product_photos_qty,
                TRIM(p.product_weight_g) AS product_weight_g,
                TRIM(p.product_length_cm) AS product_length_cm,
                TRIM(p.product_height_cm) AS product_height_cm,
                TRIM(p.product_width_cm) AS product_width_cm
            FROM bronze.products p
            LEFT JOIN bronze.category_translation t
                ON TRIM(p.product_category_name) = TRIM(t.product_category_name)
            WHERE p.product_id IS NOT NULL
        )

        INSERT INTO silver.products (
            product_id,
            product_category_name,
            product_category_name_english,
            product_name_length,
            product_description_length,
            product_photos_qty,
            product_weight_g,
            product_length_cm,
            product_height_cm,
            product_width_cm
        )
        SELECT
            product_id,

            ISNULL(NULLIF(product_category_name, ''), 'UNKNOWN'),
            ISNULL(NULLIF(product_category_name_english, ''), 'UNKNOWN'),

            ISNULL(TRY_CAST(product_name_length AS INT), 0),
            ISNULL(TRY_CAST(product_description_length AS INT), 0),
            ISNULL(TRY_CAST(product_photos_qty AS INT), 0),
            ISNULL(TRY_CAST(product_weight_g AS INT), 0),

            ISNULL(TRY_CAST(product_length_cm AS DECIMAL(10,2)), 0),
            ISNULL(TRY_CAST(product_height_cm AS DECIMAL(10,2)), 0),
            ISNULL(TRY_CAST(product_width_cm AS DECIMAL(10,2)), 0)

        FROM cleaned_data;

        SET @row_count = @@ROWCOUNT;

        -- SUCCESS LOG
        INSERT INTO etl.etl_logs (process_name, layer, status, rows_processed)
        VALUES ('usp_load_products', 'Silver', 'SUCCESS', @row_count);

    END TRY
    BEGIN CATCH

        -- ERROR LOG
        INSERT INTO etl.etl_logs (process_name, layer, status, error_message)
        VALUES ('usp_load_products', 'Silver', 'FAILED', ERROR_MESSAGE());

        THROW;

    END CATCH
END;
GO

-- 6. create silver.usp_load_payments procedure
CREATE OR ALTER PROCEDURE silver.usp_load_payments
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @row_count INT;

    BEGIN TRY

        -- START LOG
        INSERT INTO etl.etl_logs (process_name, layer, status)
        VALUES ('usp_load_payments', 'Silver', 'START');

        -- MAIN LOGIC
        TRUNCATE TABLE silver.payments;

        INSERT INTO silver.payments (
            order_id,
            payment_sequential,
            payment_type,
            payment_installments,
            payment_value
        )
        SELECT 
            REPLACE(TRIM(order_id), '"', ''),

            TRY_CAST(TRIM(payment_sequential) AS INT),

            ISNULL(
                NULLIF(LOWER(TRIM(payment_type)), ''),
                'UNKNOWN'
            ),

            ISNULL(TRY_CAST(TRIM(payment_installments) AS INT), 0),

            ISNULL(TRY_CAST(TRIM(payment_value) AS DECIMAL(10,2)), 0)

        FROM bronze.payments
        WHERE order_id IS NOT NULL;

        SET @row_count = @@ROWCOUNT;

        -- SUCCESS LOG
        INSERT INTO etl.etl_logs (process_name, layer, status, rows_processed)
        VALUES ('usp_load_payments', 'Silver', 'SUCCESS', @row_count);

    END TRY
    BEGIN CATCH

        -- ERROR LOG
        INSERT INTO etl.etl_logs (process_name, layer, status, error_message)
        VALUES ('usp_load_payments', 'Silver', 'FAILED', ERROR_MESSAGE());

        THROW;

    END CATCH
END;
GO



-- 7. create silver.usp_load_reviews procedure
CREATE OR ALTER PROCEDURE silver.usp_load_reviews
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @row_count INT;

    BEGIN TRY

        -- START LOG
        INSERT INTO etl.etl_logs (process_name, layer, status)
        VALUES ('usp_load_reviews', 'Silver', 'START');

        -- MAIN LOGIC
        TRUNCATE TABLE silver.reviews;

        WITH cleaned_data AS (
            SELECT 
                REPLACE(TRIM(review_id), '"', '') AS review_id,
                REPLACE(TRIM(order_id), '"', '') AS order_id,
                TRY_CAST(TRIM(review_score) AS INT) AS review_score,
                NULLIF(TRIM(review_comment_title), '') AS review_comment_title,
                NULLIF(TRIM(review_comment_message), '') AS review_comment_message,
                TRY_CAST(TRIM(review_creation_date) AS DATE) AS review_creation_date,
                TRY_CAST(TRIM(review_answer_timestamp) AS DATETIME2) AS review_answer_timestamp
            FROM bronze.reviews
            WHERE order_id IS NOT NULL
        )

        INSERT INTO silver.reviews (
            review_id,
            order_id,
            review_score,
            review_comment_title,
            review_comment_message,
            review_creation_date,
            review_answer_timestamp
        )
        SELECT
            review_id,
            order_id,
            CASE 
                WHEN review_score BETWEEN 1 AND 5 THEN review_score
                ELSE 0
            END,
            review_comment_title,
            review_comment_message,
            review_creation_date,
            review_answer_timestamp
        FROM cleaned_data;

        SET @row_count = @@ROWCOUNT;

        -- SUCCESS LOG
        INSERT INTO etl.etl_logs (process_name, layer, status, rows_processed)
        VALUES ('usp_load_reviews', 'Silver', 'SUCCESS', @row_count);

    END TRY
    BEGIN CATCH

        -- ERROR LOG
        INSERT INTO etl.etl_logs (process_name, layer, status, error_message)
        VALUES ('usp_load_reviews', 'Silver', 'FAILED', ERROR_MESSAGE());

        THROW;

    END CATCH
END;
GO


-- 8. create silver.usp_load_geolocation procedure
	
CREATE OR ALTER PROCEDURE silver.usp_load_geolocation
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @row_count INT;

    BEGIN TRY

        -- START LOG
        INSERT INTO etl.etl_logs (process_name, layer, status)
        VALUES ('usp_load_geolocation', 'Silver', 'START');

        -- MAIN LOGIC
        TRUNCATE TABLE silver.geolocation;

        INSERT INTO silver.geolocation (
            geolocation_zip_code_prefix,
            geolocation_lat,
            geolocation_lng,
            geolocation_city,
            geolocation_state
        )
        SELECT 
            TRY_CAST(REPLACE(TRIM(geolocation_zip_code_prefix), '"', '') AS INT),
            TRY_CAST(TRIM(geolocation_lat) AS DECIMAL(10,6)),
            TRY_CAST(TRIM(geolocation_lng) AS DECIMAL(10,6)),
            NULLIF(UPPER(TRIM(geolocation_city)), ''),
            LEFT(UPPER(TRIM(geolocation_state)), 2)
        FROM bronze.geolocation
        WHERE geolocation_zip_code_prefix IS NOT NULL;

        SET @row_count = @@ROWCOUNT;

        -- SUCCESS LOG
        INSERT INTO etl.etl_logs (process_name, layer, status, rows_processed)
        VALUES ('usp_load_geolocation', 'Silver', 'SUCCESS', @row_count);

    END TRY
    BEGIN CATCH

        -- ERROR LOG
        INSERT INTO etl.etl_logs (process_name, layer, status, error_message)
        VALUES ('usp_load_geolocation', 'Silver', 'FAILED', ERROR_MESSAGE());

        THROW;

    END CATCH
END;
GO








