-- create the silver layer tables

-- 1 silver.orders

CREATE TABLE silver.orders (
    order_id VARCHAR(50) NOT NULL,
    customer_id VARCHAR(50) NOT NULL,
    order_status VARCHAR(30) NOT NULL,
    order_purchase_timestamp DATETIME2 NOT NULL,
    order_approved_at DATETIME2 NULL,
    order_delivered_carrier_date DATETIME2 NULL,
    order_delivered_customer_date DATETIME2 NULL,
    order_estimated_delivery_date DATETIME2 NULL
);
GO


-- 2 silver.orders_items
CREATE TABLE silver.order_items (
    order_id VARCHAR(50) NOT NULL,
    order_item_id INT NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    seller_id VARCHAR(50) NOT NULL,
    shipping_limit_date DATETIME2 NULL,
    price DECIMAL(10,2) NOT NULL,
    freight_value DECIMAL(10,2) NOT NULL
);
GO

-- 3 silver.customers

CREATE TABLE silver.customers (
    customer_id VARCHAR(50) NOT NULL,
    customer_unique_id VARCHAR(50) NOT NULL,
    customer_zip_code_prefix INT NULL,
    customer_city VARCHAR(100) NULL,
    customer_state CHAR(2) NULL
);
GO


CREATE TABLE silver.sellers (
    seller_id VARCHAR(50) NOT NULL,
    seller_zip_code_prefix INT NULL,
    seller_city VARCHAR(100) NULL,
    seller_state CHAR(2) NULL
);
GO

CREATE TABLE silver.products (
    product_id VARCHAR(50) NOT NULL,
    product_category_name VARCHAR(100) NULL,
    product_category_name_english VARCHAR(100) NULL,
    product_name_length INT NULL,
    product_description_length INT NULL,
    product_photos_qty INT NULL,
    product_weight_g INT NULL,
    product_length_cm DECIMAL(10,2) NULL,
    product_height_cm DECIMAL(10,2) NULL,
    product_width_cm DECIMAL(10,2) NULL
);
GO

CREATE TABLE silver.payments (
    order_id VARCHAR(50) NOT NULL,
    payment_sequential INT NOT NULL,
    payment_type VARCHAR(30) NULL,
    payment_installments INT NULL,
    payment_value DECIMAL(10,2) NULL
);

CREATE TABLE silver.reviews (
    review_id VARCHAR(50) NOT NULL,
    order_id VARCHAR(50) NOT NULL,
    review_score INT NULL,
    review_comment_title NVARCHAR(255) NULL,
    review_comment_message NVARCHAR(MAX) NULL,
    review_creation_date DATE NULL,
    review_answer_timestamp DATETIME2 NULL
);
GO

CREATE TABLE silver.geolocation (
    geolocation_zip_code_prefix INT NOT NULL,
    geolocation_city VARCHAR(100) NULL,
    geolocation_state CHAR(2) NULL,
    geolocation_lat DECIMAL(10,6) NULL,
    geolocation_lng DECIMAL(10,6) NULL
);
GO


CREATE TABLE gold.dim_seller (
    seller_key INT IDENTITY(1,1) PRIMARY KEY,
    seller_id VARCHAR(50) NOT NULL,
    seller_zip_code_prefix INT NULL,
    seller_city VARCHAR(100) NULL,
    seller_state VARCHAR(10) NULL,
    created_date DATETIME NOT NULL,
    updated_date DATETIME NOT NULL
);
GO

