
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
	        TRIM(REPLACE(order_id,'"','')) AS order_id,
            TRY_CAST(order_item_id AS INT) AS order_item_id,
            TRIM(REPLACE(product_id,'"','')) AS product_id,
            TRIM(REPLACE(seller_id,'"','')) AS seller_id,
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



-- create silver.usp_load_orders procedure

CREATE OR ALTER PROCEDURE silver.usp_load_sellers
AS
    BEGIN

        TRUNCATE TABLE silver.sellers;

        INSERT INTO silver.sellers (
            seller_id,
            seller_zip_code_prefix,
            seller_city,
            seller_state
        )

        SELECT 
	        TRIM(REPLACE(seller_id,'"','')) AS seller_id,
	        TRY_CAST(TRIM(REPLACE(seller_zip_code_prefix,'"','')) AS INT ) AS seller_zip_code_prefix,
	        UPPER(TRIM(seller_city)) AS seller_city,
	        UPPER(RIGHT(TRIM(REPLACE(seller_state,'"','')),2)) AS seller_state
        FROM bronze.sellers;

    END;
GO








CREATE OR ALTER PROCEDURE silver.usp_load_products
AS
    BEGIN

        TRUNCATE TABLE silver.products;

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
            REPLACE(TRIM(p.product_id), '"', '') AS product_id,

            CASE
                WHEN p.product_category_name IS NULL OR TRIM(p.product_category_name) = '' THEN 'UNKNOWN'
                ELSE TRIM(p.product_category_name)
            END AS product_category_name,

            CASE
                WHEN t.product_category_name_english IS NULL OR TRIM(t.product_category_name_english) = '' THEN 'UNKNOWN'
                ELSE TRIM(t.product_category_name_english)
            END AS product_category_name_english,

            CASE
                WHEN p.product_name_lenght IS NULL OR TRIM(p.product_name_lenght) = '' THEN 0
                ELSE TRY_CAST(TRIM(p.product_name_lenght) AS INT)
            END AS product_name_lenght,



            CASE
                WHEN p.product_description_lenght IS NULL OR TRIM(p.product_description_lenght) = '' THEN 0
                ELSE TRY_CAST(TRIM(p.product_description_lenght) AS INT)
            END AS product_description_lenght,

            CASE
                WHEN p.product_photos_qty IS NULL OR TRIM(p.product_photos_qty) = '' THEN 0
                ELSE TRY_CAST(TRIM(p.product_photos_qty) AS INT)
            END AS product_photos_qty,

            CASE
                WHEN p.product_weight_g IS NULL OR TRIM(p.product_weight_g) = '' THEN 0
                ELSE TRY_CAST(TRIM(p.product_weight_g) AS INT)
            END AS product_weight_g,

            CASE
                WHEN p.product_length_cm IS NULL OR TRIM(p.product_length_cm) = '' THEN 0
                ELSE TRY_CAST(TRIM(p.product_length_cm) AS DECIMAL(10,2))
            END AS product_length_cm,

            CASE
                WHEN p.product_height_cm IS NULL OR TRIM(p.product_height_cm) = '' THEN 0
                ELSE TRY_CAST(TRIM(p.product_height_cm) AS DECIMAL(10,2))
            END AS product_height_cm,

            CASE
                WHEN p.product_width_cm IS NULL OR TRIM(p.product_width_cm) = '' THEN 0
                ELSE TRY_CAST(TRIM(p.product_width_cm) AS DECIMAL(10,2))
            END AS product_width_cm
        FROM bronze.products AS p
        LEFT JOIN bronze.category_translation AS t
        ON TRIM(p.product_category_name) = TRIM(t.product_category_name);
    END;
GO



CREATE OR ALTER PROCEDURE silver.usp_load_payments
AS
    BEGIN

        TRUNCATE TABLE silver.payments;


        INSERT INTO silver.payments (
            order_id,
            payment_sequential,
            payment_type,
            payment_installments,
            payment_value
        )


        SELECT 
            REPLACE(TRIM(order_id), '"', '') AS order_id,

            TRY_CAST(TRIM(payment_sequential) AS INT) AS payment_sequential,

            CASE 
                WHEN payment_type IS NULL OR TRIM(payment_type) = '' THEN 'UNKNOWN'
                WHEN LOWER(TRIM(payment_type)) = 'credit_card' THEN 'credit_card'
                WHEN LOWER(TRIM(payment_type)) = 'debit_card' THEN 'debit_card'
                WHEN LOWER(TRIM(payment_type)) = 'voucher' THEN 'voucher'
                WHEN LOWER(TRIM(payment_type)) = 'boleto' THEN 'boleto'
                ELSE 'UNKNOWN'
            END AS payment_type,

            CASE
                WHEN payment_installments IS NULL OR TRIM(payment_installments) = '' THEN 0
                ELSE TRY_CAST(TRIM(payment_installments) AS INT)
            END AS payment_installments,

            CASE
                WHEN payment_value IS NULL OR TRIM(payment_value) = '' THEN 0
                ELSE TRY_CAST(TRIM(payment_value) AS DECIMAL(10,2))
            END AS payment_value

        FROM bronze.payments;
    END;
GO




CREATE OR ALTER PROCEDURE silver.usp_load_reviews
AS
    BEGIN

        TRUNCATE TABLE silver.reviews;

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
	        REPLACE(TRIM(review_id),'"','') AS review_id,
	        REPLACE(TRIM(order_id),'"','') AS order_id,
            CASE 
                WHEN TRY_CAST(TRIM(review_score) AS INT) BETWEEN 1 AND 5 
                    THEN TRY_CAST(TRIM(review_score) AS INT)
                ELSE 0
            END AS review_score,

            NULLIF(TRIM(review_comment_title), '') AS review_comment_title,
            NULLIF(TRIM(review_comment_message), '') AS review_comment_message,
            TRY_CAST(TRIM(review_creation_date) AS DATE) AS review_creation_date,
            TRY_CAST(TRIM(review_answer_timestamp) AS DATETIME2) AS review_answer_timestamp
        FROM bronze.reviews;
    END;
GO



CREATE OR ALTER PROCEDURE silver.usp_load_geolocation
AS
    BEGIN

        TRUNCATE TABLE silver.geolocation;

        INSERT INTO silver.geolocation (
            geolocation_zip_code_prefix,
            geolocation_lat,
            geolocation_lng,
            geolocation_city,
            geolocation_state
        )

        SELECT 
            TRY_CAST(REPLACE(TRIM(geolocation_zip_code_prefix), '"', '') AS INT) AS geolocation_zip_code_prefix,
            TRY_CAST(TRIM(geolocation_lat) AS DECIMAL(10,6)) AS geolocation_lat,
            TRY_CAST(TRIM(geolocation_lng) AS DECIMAL(10,6)) AS geolocation_lng,
            NULLIF(UPPER(TRIM(geolocation_city)), '') AS geolocation_city,
            TRY_CAST(UPPER(TRIM(geolocation_state)) AS CHAR(2)) AS geolocation_state
        FROM bronze.geolocation;
    END;
GO











