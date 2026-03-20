-- create FactSales

CREATE TABLE gold.fact_sales (
    sales_key BIGINT IDENTITY(1,1) PRIMARY KEY,

    -- Business Keys
    order_id VARCHAR(50) NOT NULL,
    order_item_id INT NOT NULL,

    -- Foreign Keys
    customer_key INT NOT NULL,
    seller_key INT NOT NULL,
    product_key INT NOT NULL,
    order_date_key INT NOT NULL,

    -- Measures
    price DECIMAL(10,2) NOT NULL,
    freight_value DECIMAL(10,2) NOT NULL,
    total_sales_amount AS (price + freight_value), -- computed column

    -- Derived Metrics
    delivery_days INT NULL,

    -- Additional Info
    order_status VARCHAR(50),
    order_purchase_timestamp DATETIME2,
    order_delivered_customer_date DATETIME2
);
GO
