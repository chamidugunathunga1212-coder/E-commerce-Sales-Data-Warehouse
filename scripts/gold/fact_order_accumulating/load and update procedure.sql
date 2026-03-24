CREATE OR ALTER PROCEDURE gold.usp_load_fact_accumulating
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @row_count INT = 0;

    BEGIN TRY

        ---------------------------------------------------
        -- START LOG
        ---------------------------------------------------
        INSERT INTO etl.etl_logs (process_name, layer, status)
        VALUES ('usp_load_fact_accumulating', 'Gold', 'START');

        ---------------------------------------------------
        -- 1. INSERT NEW ORDERS (ONE ROW PER ORDER)
        ---------------------------------------------------
        INSERT INTO gold.fact_order_accumulating (
            order_id,
            customer_key,
            seller_key,
            create_time,
            complete_time,
            process_hours,
            order_status
        )
        SELECT 
            o.order_id,

            ISNULL(c.customer_key, -1),

            ISNULL(s.seller_key, -1),

            o.order_purchase_timestamp, 
            NULL,                       
            NULL,                       
            o.order_status

        FROM silver.orders o

        ---------------------------------------------------
        -- SCD CUSTOMER JOIN
        ---------------------------------------------------
        LEFT JOIN gold.dim_customer c
            ON o.customer_id = c.customer_id
            AND o.order_purchase_timestamp >= c.start_date
            AND (o.order_purchase_timestamp < c.end_date OR c.end_date IS NULL)

        ---------------------------------------------------
        --  SELLER JOIN (ONE SELLER PER ORDER)
        ---------------------------------------------------
        LEFT JOIN (
            SELECT 
                oi.order_id,
                MIN(s.seller_key) AS seller_key
            FROM silver.order_items oi
            LEFT JOIN gold.dim_seller s
                ON oi.seller_id = s.seller_id
            GROUP BY oi.order_id
        ) s
            ON o.order_id = s.order_id

        ---------------------------------------------------
        -- ONLY NEW ORDERS
        ---------------------------------------------------
        WHERE NOT EXISTS (
            SELECT 1
            FROM gold.fact_order_accumulating f
            WHERE f.order_id = o.order_id
        );

        SET @row_count = @row_count + @@ROWCOUNT;

        ---------------------------------------------------
        -- 2. UPDATE EXISTING ORDERS (ACCUMULATING LOGIC)
        ---------------------------------------------------
        UPDATE f
        SET 
            f.complete_time = o.order_delivered_customer_date,

            f.process_hours = 
                DATEDIFF(
                    HOUR,
                    f.create_time,
                    o.order_delivered_customer_date
                ),

            f.order_status = o.order_status

        FROM gold.fact_order_accumulating f
        JOIN silver.orders o
            ON f.order_id = o.order_id

        WHERE 
            o.order_delivered_customer_date IS NOT NULL
            AND (
                f.complete_time IS NULL 
                OR f.complete_time <> o.order_delivered_customer_date
            );

        SET @row_count = @row_count + @@ROWCOUNT;

        ---------------------------------------------------
        -- SUCCESS LOG
        ---------------------------------------------------
        INSERT INTO etl.etl_logs (
            process_name,
            layer,
            status,
            rows_processed
        )
        VALUES (
            'usp_load_fact_accumulating',
            'Gold',
            'SUCCESS',
            @row_count
        );

    END TRY
    BEGIN CATCH

        ---------------------------------------------------
        -- ERROR LOG
        ---------------------------------------------------
        INSERT INTO etl.etl_logs (
            process_name,
            layer,
            status,
            error_message
        )
        VALUES (
            'usp_load_fact_accumulating',
            'Gold',
            'FAILED',
            ERROR_MESSAGE()
        );

        THROW;

    END CATCH
END;
GO





-- insert part 

INSERT INTO gold.fact_order_accumulating
SELECT DISTINCT
    o.order_id,

    ISNULL(c.customer_key, -1),


    ISNULL(s.seller_key, -1),

    o.order_purchase_timestamp, 
    NULL,                       
    NULL,                       
    o.order_status

FROM silver.orders o

LEFT JOIN gold.dim_customer c
    ON o.customer_id = c.customer_id
    AND o.order_purchase_timestamp >= c.start_date
    AND (o.order_purchase_timestamp < c.end_date OR c.end_date IS NULL)


LEFT JOIN (
    SELECT 
        oi.order_id,
        MIN(s.seller_key) AS seller_key
    FROM silver.order_items oi
    LEFT JOIN gold.dim_seller s
        ON oi.seller_id = s.seller_id
    GROUP BY oi.order_id
) s
    ON o.order_id = s.order_id

WHERE NOT EXISTS (
    SELECT 1 
    FROM gold.fact_order_accumulating f
    WHERE f.order_id = o.order_id
);
