CREATE TABLE gold.fact_order_accumulating (
    order_id VARCHAR(50) PRIMARY KEY,

    customer_key INT,
    seller_key INT,

    create_time DATETIME2,
    complete_time DATETIME2,

    process_hours INT,

    order_status VARCHAR(30)
);
