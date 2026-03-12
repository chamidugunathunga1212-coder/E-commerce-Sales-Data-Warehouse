-- create DimCustomer

CREATE TABLE gold.dim_customer (
    customer_key INT IDENTITY(1,1) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    customer_unique_id VARCHAR(50) NULL,
    customer_zip_code_prefix INT NULL,
    customer_city VARCHAR(100) NULL,
    customer_state CHAR(2) NULL
);
GO


-- create DimSeller

CREATE TABLE gold.dim_seller (
    seller_key INT IDENTITY(1,1) PRIMARY KEY,
    seller_id VARCHAR(50) NOT NULL,
    seller_zip_code_prefix INT NULL,
    seller_city VARCHAR(100) NULL,
    seller_state CHAR(2) NULL
);
GO



-- create DimProduct

CREATE TABLE gold.dim_product (
    product_key INT IDENTITY(1,1) PRIMARY KEY,
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


-- create DimDate

CREATE TABLE gold.dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    day_number INT NOT NULL,
    month_number INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    quarter_number INT NOT NULL,
    year_number INT NOT NULL,
    weekday_name VARCHAR(20) NOT NULL
);
GO
