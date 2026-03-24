CREATE TABLE gold.fact_order_accumulating (
    order_id VARCHAR(50) PRIMARY KEY,

    customer_key INT,
    seller_key INT,

    accm_txn_create_time DATETIME2,
    accm_txn_complete_time DATETIME2,

    txn_process_time_hours INT,

    order_status VARCHAR(30)
);
