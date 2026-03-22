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




-- sales facts procedure 
CREATE OR ALTER PROCEDURE gold.usp_load_fact_sales
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @row_count INT;

    BEGIN TRY

        -- START LOG
        INSERT INTO etl.etl_logs (process_name, layer, status)
        VALUES ('usp_load_fact_sales', 'Gold', 'START');

        -- MERGE (INSERT ONLY NEW RECORDS)
        MERGE gold.fact_sales AS target
        USING (
            SELECT 
                oi.order_id,
                oi.order_item_id,

                c.customer_key,
                s.seller_key,
                p.product_key,

                CONVERT(INT, CONVERT(CHAR(8), o.order_purchase_timestamp, 112)) AS order_date_key,

                oi.price,
                oi.freight_value,

                CASE 
                    WHEN o.order_delivered_customer_date IS NOT NULL 
                         AND o.order_purchase_timestamp IS NOT NULL
                    THEN DATEDIFF(
                            DAY,
                            o.order_purchase_timestamp,
                            o.order_delivered_customer_date
                         )
                    ELSE NULL
                END AS delivery_days,

                o.order_status,
                o.order_purchase_timestamp,
                o.order_delivered_customer_date

            FROM silver.order_items oi

            LEFT JOIN silver.orders o
                ON oi.order_id = o.order_id

            LEFT JOIN gold.dim_customer c
                ON o.customer_id = c.customer_id

            LEFT JOIN gold.dim_seller s
                ON oi.seller_id = s.seller_id

            LEFT JOIN gold.dim_product p
                ON oi.product_id = p.product_id

            WHERE oi.order_id IS NOT NULL

        ) AS source

        ON target.order_id = source.order_id
        AND target.order_item_id = source.order_item_id

        WHEN NOT MATCHED THEN
            INSERT (
                order_id,
                order_item_id,
                customer_key,
                seller_key,
                product_key,
                order_date_key,
                price,
                freight_value,
                delivery_days,
                order_status,
                order_purchase_timestamp,
                order_delivered_customer_date
            )
            VALUES (
                source.order_id,
                source.order_item_id,
                source.customer_key,
                source.seller_key,
                source.product_key,
                source.order_date_key,
                source.price,
                source.freight_value,
                source.delivery_days,
                source.order_status,
                source.order_purchase_timestamp,
                source.order_delivered_customer_date
            );

        SET @row_count = @@ROWCOUNT;

        -- SUCCESS LOG
        INSERT INTO etl.etl_logs (
            process_name, layer, status, rows_processed
        )
        VALUES (
            'usp_load_fact_sales', 'Gold', 'SUCCESS', @row_count
        );

    END TRY
    BEGIN CATCH

        -- ERROR LOG
        INSERT INTO etl.etl_logs (
            process_name, layer, status, error_message
        )
        VALUES (
            'usp_load_fact_sales', 'Gold', 'FAILED', ERROR_MESSAGE()
        );

        THROW;

    END CATCH
END;
GO
