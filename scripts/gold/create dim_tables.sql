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
IF OBJECT_ID('gold.dim_date', 'U') IS NOT NULL
    DROP TABLE gold.dim_date;
GO

CREATE TABLE gold.dim_date (
    date_key            INT PRIMARY KEY,
    full_date           DATE NOT NULL,
    day_number          INT NOT NULL,
    month_number        INT NOT NULL,
    month_name          VARCHAR(20) NOT NULL,
    short_month_name    CHAR(3) NOT NULL,
    quarter_number      INT NOT NULL,
    year_number         INT NOT NULL,
    weekday_number      INT NOT NULL,
    weekday_name        VARCHAR(20) NOT NULL,
    short_weekday_name  CHAR(3) NOT NULL,
    week_of_year        INT NOT NULL,
    is_weekend          BIT NOT NULL,
    is_month_start      BIT NOT NULL,
    is_month_end        BIT NOT NULL,
    is_year_start       BIT NOT NULL,
    is_year_end         BIT NOT NULL,
    month_start_date    DATE NOT NULL,
    month_end_date      DATE NOT NULL,
    year_start_date     DATE NOT NULL,
    year_end_date       DATE NOT NULL,
    year_month          CHAR(7) NOT NULL,
    year_week           VARCHAR(8) NOT NULL,
);
GO
