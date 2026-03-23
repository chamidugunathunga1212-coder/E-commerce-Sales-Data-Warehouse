CREATE OR ALTER PROCEDURE gold.usp_load_fact_sales
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @row_count INT;

    BEGIN TRY

        ---------------------------------------------------
        -- START LOG
        ---------------------------------------------------
        INSERT INTO etl.etl_logs (process_name, layer, status)
        VALUES ('usp_load_fact_sales', 'Gold', 'START');

        ---------------------------------------------------
        -- LOAD FACT TABLE (SCD AWARE)
        ---------------------------------------------------
        MERGE gold.fact_sales AS target
        USING (
            SELECT 
                oi.order_id,
                oi.order_item_id,

                --  SCD CUSTOMER KEY
                ISNULL(c.customer_key, -1) AS customer_key,

                --  TYPE 1 SELLER
                ISNULL(s.seller_key, -1) AS seller_key,

                --  SCD PRODUCT KEY
                ISNULL(p.product_key, -1) AS product_key,

                -- DATE KEY
                CONVERT(INT, CONVERT(CHAR(8), o.order_purchase_timestamp, 112)) AS order_date_key,

                oi.price,
                oi.freight_value,

                -- DELIVERY DAYS
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

            ---------------------------------------------------
            --  SCD CUSTOMER JOIN (CRITICAL FIX)
            ---------------------------------------------------
            LEFT JOIN gold.dim_customer c
                ON o.customer_id = c.customer_id
                AND o.order_purchase_timestamp >= c.start_date
                AND (o.order_purchase_timestamp < c.end_date OR c.end_date IS NULL)

            ---------------------------------------------------
            --  TYPE 1 SELLER JOIN
            ---------------------------------------------------
            LEFT JOIN gold.dim_seller s
                ON oi.seller_id = s.seller_id

            ---------------------------------------------------
            --  SCD PRODUCT JOIN (CRITICAL FIX)
            ---------------------------------------------------
            LEFT JOIN gold.dim_product p
                ON oi.product_id = p.product_id
                AND o.order_purchase_timestamp >= p.start_date
                AND (o.order_purchase_timestamp < p.end_date OR p.end_date IS NULL)

            WHERE oi.order_id IS NOT NULL

        ) AS source

        ON target.order_id = source.order_id
        AND target.order_item_id = source.order_item_id

        ---------------------------------------------------
        -- INSERT ONLY NEW RECORDS
        ---------------------------------------------------
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

        ---------------------------------------------------
        SET @row_count = @@ROWCOUNT;

        ---------------------------------------------------
        -- SUCCESS LOG
        ---------------------------------------------------
        INSERT INTO etl.etl_logs (
            process_name, layer, status, rows_processed
        )
        VALUES (
            'usp_load_fact_sales', 'Gold', 'SUCCESS', @row_count
        );

    END TRY
    BEGIN CATCH

        ---------------------------------------------------
        -- ERROR LOG
        ---------------------------------------------------
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



