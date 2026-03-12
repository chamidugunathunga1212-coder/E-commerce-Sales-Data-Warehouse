-- create FactSales

CREATE TABLE gold.fact_sales (
    sales_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    order_item_id INT NOT NULL,
    customer_key INT NOT NULL,
    seller_key INT NOT NULL,
    product_key INT NOT NULL,
    order_date_key INT NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    freight_value DECIMAL(10,2) NOT NULL,
    total_sales_amount DECIMAL(10,2) NOT NULL,
    delivery_days INT NULL
);
GO

