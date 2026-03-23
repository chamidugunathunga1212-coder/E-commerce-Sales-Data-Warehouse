CREATE OR ALTER PROCEDURE gold.usp_load_dim_product
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @row_count INT;

    BEGIN TRY

        -- START LOG
        INSERT INTO etl.etl_logs (process_name, layer, status)
        VALUES ('usp_load_dim_product', 'Gold', 'START');

        ---------------------------------------------------
        -- 1. INSERT NEW PRODUCTS
        ---------------------------------------------------
        INSERT INTO gold.dim_product (
            product_id,
            product_category_name,
            product_category_name_english,
            product_name_length,
            product_description_length,
            product_photos_qty,
            product_weight_g,
            product_length_cm,
            product_height_cm,
            product_width_cm,
            start_date,
            end_date,
            is_current
        )
        SELECT 
            s.product_id,
            s.product_category_name,
            s.product_category_name_english,
            s.product_name_length,
            s.product_description_length,
            s.product_photos_qty,
            s.product_weight_g,
            s.product_length_cm,
            s.product_height_cm,
            s.product_width_cm,
            '2015-01-01',
            NULL,
            1
        FROM silver.products s
        LEFT JOIN gold.dim_product d
            ON s.product_id = d.product_id
            AND d.is_current = 1
        WHERE d.product_id IS NULL;

        ---------------------------------------------------
        -- 2. EXPIRE OLD RECORDS
        ---------------------------------------------------
        UPDATE d
        SET 
            d.end_date = GETDATE(),
            d.is_current = 0
        FROM gold.dim_product d
        JOIN silver.products s
            ON d.product_id = s.product_id
        WHERE d.is_current = 1
        AND (
            ISNULL(d.product_category_name,'') <> ISNULL(s.product_category_name,'')
            OR ISNULL(d.product_category_name_english,'') <> ISNULL(s.product_category_name_english,'')
            OR ISNULL(d.product_weight_g,0) <> ISNULL(s.product_weight_g,0)
        );

        ---------------------------------------------------
        -- 3. INSERT NEW VERSION
        ---------------------------------------------------
        INSERT INTO gold.dim_product (
            product_id,
            product_category_name,
            product_category_name_english,
            product_name_length,
            product_description_length,
            product_photos_qty,
            product_weight_g,
            product_length_cm,
            product_height_cm,
            product_width_cm,
            start_date,
            end_date,
            is_current
        )
        SELECT 
            s.product_id,
            s.product_category_name,
            s.product_category_name_english,
            s.product_name_length,
            s.product_description_length,
            s.product_photos_qty,
            s.product_weight_g,
            s.product_length_cm,
            s.product_height_cm,
            s.product_width_cm,
            GETDATE(),
            NULL,
            1
        FROM silver.products s
        JOIN gold.dim_product d
            ON s.product_id = d.product_id
        WHERE d.is_current = 0
        AND d.end_date = (
            SELECT MAX(d2.end_date)
            FROM gold.dim_product d2
            WHERE d2.product_id = d.product_id
        );

        ---------------------------------------------------
        SET @row_count = @@ROWCOUNT;

        INSERT INTO etl.etl_logs (process_name, layer, status, rows_processed)
        VALUES ('usp_load_dim_product', 'Gold', 'SUCCESS', @row_count);

    END TRY
    BEGIN CATCH
        INSERT INTO etl.etl_logs (process_name, layer, status, error_message)
        VALUES ('usp_load_dim_product', 'Gold', 'FAILED', ERROR_MESSAGE());
    END CATCH
END;


