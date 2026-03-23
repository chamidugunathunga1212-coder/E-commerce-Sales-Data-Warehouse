CREATE OR ALTER PROCEDURE gold.usp_load_dim_customer
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @row_count INT;

    BEGIN TRY

        -- START LOG
        INSERT INTO etl.etl_logs (process_name, layer, status)
        VALUES ('usp_load_dim_customer', 'Gold', 'START');

        ---------------------------------------------------
        -- 1. INSERT NEW CUSTOMERS
        ---------------------------------------------------
        INSERT INTO gold.dim_customer (
            customer_id,
            customer_unique_id,
            customer_zip_code_prefix,
            customer_city,
            customer_state,
            start_date,
            end_date,
            is_current
        )
        SELECT 
            s.customer_id,
            s.customer_unique_id,
            s.customer_zip_code_prefix,
            s.customer_city,
            s.customer_state,
            '2015-01-01', 
            NULL,
            1
        FROM silver.customers s
        LEFT JOIN gold.dim_customer d
            ON s.customer_id = d.customer_id
            AND d.is_current = 1
        WHERE d.customer_id IS NULL;

        ---------------------------------------------------
        -- 2. EXPIRE OLD RECORDS
        ---------------------------------------------------
        UPDATE d
        SET 
            d.end_date = GETDATE(),
            d.is_current = 0
        FROM gold.dim_customer d
        JOIN silver.customers s
            ON d.customer_id = s.customer_id
        WHERE d.is_current = 1
        AND (
            ISNULL(d.customer_city,'') <> ISNULL(s.customer_city,'')
            OR ISNULL(d.customer_state,'') <> ISNULL(s.customer_state,'')
        );

        ---------------------------------------------------
        -- 3. INSERT NEW VERSION
        ---------------------------------------------------
        INSERT INTO gold.dim_customer (
            customer_id,
            customer_unique_id,
            customer_zip_code_prefix,
            customer_city,
            customer_state,
            start_date,
            end_date,
            is_current
        )
        SELECT 
            s.customer_id,
            s.customer_unique_id,
            s.customer_zip_code_prefix,
            s.customer_city,
            s.customer_state,
            GETDATE(),   -- ✔ correct for change tracking
            NULL,
            1
        FROM silver.customers s
        JOIN gold.dim_customer d
            ON s.customer_id = d.customer_id
        WHERE d.is_current = 0
        AND d.end_date = (
            SELECT MAX(d2.end_date)
            FROM gold.dim_customer d2
            WHERE d2.customer_id = d.customer_id
        );

        ---------------------------------------------------
        SET @row_count = @@ROWCOUNT;

        INSERT INTO etl.etl_logs (process_name, layer, status, rows_processed)
        VALUES ('usp_load_dim_customer', 'Gold', 'SUCCESS', @row_count);

    END TRY
    BEGIN CATCH
        INSERT INTO etl.etl_logs (process_name, layer, status, error_message)
        VALUES ('usp_load_dim_customer', 'Gold', 'FAILED', ERROR_MESSAGE());
    END CATCH
END;




