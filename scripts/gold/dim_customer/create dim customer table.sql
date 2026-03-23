CREATE TABLE gold.dim_customer (
    customer_key INT IDENTITY(1,1) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,  -- ❌ no UNIQUE
    customer_unique_id VARCHAR(50),
    customer_zip_code_prefix INT,
    customer_city VARCHAR(100),
    customer_state VARCHAR(10),

    start_date DATETIME NOT NULL,
    end_date DATETIME NULL,
    is_current BIT NOT NULL
);
