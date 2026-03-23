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


-- dim_customer

CREATE TABLE gold.dim_customer (
    customer_key INT IDENTITY(1,1) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,  
    customer_unique_id VARCHAR(50),
    customer_zip_code_prefix INT,
    customer_city VARCHAR(100),
    customer_state VARCHAR(10),

    start_date DATETIME NOT NULL,
    end_date DATETIME NULL,
    is_current BIT NOT NULL
);

GO
-- dim_seller

CREATE TABLE gold.dim_seller (
    seller_key INT IDENTITY(1,1) PRIMARY KEY,
    seller_id VARCHAR(50) NOT NULL UNIQUE,
    seller_zip_code_prefix INT,
    seller_city VARCHAR(100),
    seller_state CHAR(2),
    created_date DATETIME DEFAULT GETDATE(),
    updated_date DATETIME DEFAULT GETDATE()
);

GO 

-- dim_product

CREATE TABLE gold.dim_product (
    product_key INT IDENTITY(1,1) PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL UNIQUE,
    product_category_name VARCHAR(100),
    product_category_name_english VARCHAR(100),
    product_name_length INT,
    product_description_length INT,
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm DECIMAL(10,2),
    product_height_cm DECIMAL(10,2),
    product_width_cm DECIMAL(10,2),
    created_date DATETIME DEFAULT GETDATE(),
    updated_date DATETIME DEFAULT GETDATE()
);



