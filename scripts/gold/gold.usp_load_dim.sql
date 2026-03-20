
-- gold.usp_load_dim_date

CREATE OR ALTER PROCEDURE gold.usp_load_dim_date
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE gold.dim_date;

    DECLARE @start_date DATE = '2015-01-01';
    DECLARE @end_date   DATE = '2030-12-31';

    ;WITH date_series AS (
        SELECT @start_date AS full_date

        UNION ALL

        SELECT DATEADD(DAY, 1, full_date)
        FROM date_series
        WHERE full_date < @end_date
    )
    INSERT INTO gold.dim_date (
        date_key,
        full_date,
        day_number,
        month_number,
        month_name,
        short_month_name,
        quarter_number,
        year_number,
        weekday_number,
        weekday_name,
        short_weekday_name,
        week_of_year,
        is_weekend,
        is_month_start,
        is_month_end,
        is_year_start,
        is_year_end,
        month_start_date,
        month_end_date,
        year_start_date,
        year_end_date,
        year_month,
        year_week
    )
    SELECT
        CONVERT(INT, CONVERT(CHAR(8), full_date, 112)) AS date_key,
        full_date,
        DAY(full_date) AS day_number,
        MONTH(full_date) AS month_number,
        DATENAME(MONTH, full_date) AS month_name,
        LEFT(DATENAME(MONTH, full_date), 3) AS short_month_name,
        DATEPART(QUARTER, full_date) AS quarter_number,
        YEAR(full_date) AS year_number,
        DATEPART(WEEKDAY, full_date) AS weekday_number,
        DATENAME(WEEKDAY, full_date) AS weekday_name,
        LEFT(DATENAME(WEEKDAY, full_date), 3) AS short_weekday_name,
        DATEPART(WEEK, full_date) AS week_of_year,
        CASE
            WHEN DATENAME(WEEKDAY, full_date) IN ('Saturday', 'Sunday') THEN 1
            ELSE 0
        END AS is_weekend,
        CASE
            WHEN full_date = DATEFROMPARTS(YEAR(full_date), MONTH(full_date), 1) THEN 1
            ELSE 0
        END AS is_month_start,
        CASE
            WHEN full_date = EOMONTH(full_date) THEN 1
            ELSE 0
        END AS is_month_end,
        CASE
            WHEN full_date = DATEFROMPARTS(YEAR(full_date), 1, 1) THEN 1
            ELSE 0
        END AS is_year_start,
        CASE
            WHEN full_date = DATEFROMPARTS(YEAR(full_date), 12, 31) THEN 1
            ELSE 0
        END AS is_year_end,
        DATEFROMPARTS(YEAR(full_date), MONTH(full_date), 1) AS month_start_date,
        EOMONTH(full_date) AS month_end_date,
        DATEFROMPARTS(YEAR(full_date), 1, 1) AS year_start_date,
        DATEFROMPARTS(YEAR(full_date), 12, 31) AS year_end_date,
        CONVERT(CHAR(7), full_date, 120) AS year_month,
        CONCAT(YEAR(full_date), '-', RIGHT('0' + CAST(DATEPART(WEEK, full_date) AS VARCHAR(2)), 2)) AS year_week
    FROM date_series
    OPTION (MAXRECURSION 0);
END;
GO




-- gold.usp_load_dim_customer

CREATE OR ALTER PROCEDURE gold.usp_load_dim_customer
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY

        INSERT INTO etl.etl_logs (process_name, layer, status)
        VALUES ('usp_load_dim_customer', 'Gold', 'START');

        MERGE gold.dim_customer AS target
        USING (
            SELECT DISTINCT
                customer_id,
                customer_unique_id,
                customer_zip_code_prefix,
                customer_city,
                customer_state
            FROM silver.customers
            WHERE customer_id IS NOT NULL
        ) AS source
        ON target.customer_id = source.customer_id

        WHEN MATCHED THEN
            UPDATE SET
                target.customer_unique_id = source.customer_unique_id,
                target.customer_zip_code_prefix = source.customer_zip_code_prefix,
                target.customer_city = source.customer_city,
                target.customer_state = source.customer_state,
                target.updated_date = GETDATE()

        WHEN NOT MATCHED THEN
            INSERT (
                customer_id,
                customer_unique_id,
                customer_zip_code_prefix,
                customer_city,
                customer_state,
                created_date,
                updated_date
            )
            VALUES (
                source.customer_id,
                source.customer_unique_id,
                source.customer_zip_code_prefix,
                source.customer_city,
                source.customer_state,
                GETDATE(),
                GETDATE()
            );

        INSERT INTO etl.etl_logs (process_name, layer, status)
        VALUES ('usp_load_dim_customer', 'Gold', 'SUCCESS');

    END TRY
    BEGIN CATCH

        INSERT INTO etl.etl_logs (process_name, layer, status, error_message)
        VALUES ('usp_load_dim_customer', 'Gold', 'FAILED', ERROR_MESSAGE());

        THROW;

    END CATCH
END;
GO








-- gold.usp_load_dim_product

CREATE OR ALTER PROCEDURE gold.usp_load_dim_product
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @row_count INT;

    BEGIN TRY

        -- START LOG
        INSERT INTO etl.etl_logs (process_name, layer, status)
        VALUES ('usp_load_dim_product', 'Gold', 'START');

        -- MERGE (UPSERT)
        MERGE gold.dim_product AS target
        USING (
            SELECT DISTINCT
                product_id,
                product_category_name,
                product_category_name_english,
                product_name_length,
                product_description_length,
                product_photos_qty,
                product_weight_g,
                product_length_cm,
                product_height_cm,
                product_width_cm
            FROM silver.products
            WHERE product_id IS NOT NULL
        ) AS source
        ON target.product_id = source.product_id

        WHEN MATCHED THEN
            UPDATE SET
                target.product_category_name = source.product_category_name,
                target.product_category_name_english = source.product_category_name_english,
                target.product_name_length = source.product_name_length,
                target.product_description_length = source.product_description_length,
                target.product_photos_qty = source.product_photos_qty,
                target.product_weight_g = source.product_weight_g,
                target.product_length_cm = source.product_length_cm,
                target.product_height_cm = source.product_height_cm,
                target.product_width_cm = source.product_width_cm,
                target.updated_date = GETDATE()

        WHEN NOT MATCHED THEN
            INSERT (
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
                created_date,
                updated_date
            )
            VALUES (
                source.product_id,
                source.product_category_name,
                source.product_category_name_english,
                source.product_name_length,
                source.product_description_length,
                source.product_photos_qty,
                source.product_weight_g,
                source.product_length_cm,
                source.product_height_cm,
                source.product_width_cm,
                GETDATE(),
                GETDATE()
            );

        SET @row_count = @@ROWCOUNT;

        -- SUCCESS LOG
        INSERT INTO etl.etl_logs (
            process_name, layer, status, rows_processed
        )
        VALUES (
            'usp_load_dim_product', 'Gold', 'SUCCESS', @row_count
        );

    END TRY
    BEGIN CATCH

        -- ERROR LOG
        INSERT INTO etl.etl_logs (
            process_name, layer, status, error_message
        )
        VALUES (
            'usp_load_dim_product', 'Gold', 'FAILED', ERROR_MESSAGE()
        );

        THROW;

    END CATCH
END;
GO


-- gold.usp_load_dim_seller

CREATE OR ALTER PROCEDURE gold.usp_load_dim_seller
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @row_count INT;

    BEGIN TRY

        -- START LOG
        INSERT INTO etl.etl_logs (process_name, layer, status)
        VALUES ('usp_load_dim_seller', 'Gold', 'START');

        -- MERGE (UPSERT)
        MERGE gold.dim_seller AS target
        USING (
            SELECT DISTINCT
                seller_id,
                seller_zip_code_prefix,
                seller_city,
                seller_state
            FROM silver.sellers
            WHERE seller_id IS NOT NULL
        ) AS source
        ON target.seller_id = source.seller_id

        WHEN MATCHED THEN
            UPDATE SET
                target.seller_zip_code_prefix = source.seller_zip_code_prefix,
                target.seller_city = source.seller_city,
                target.seller_state = source.seller_state,
                target.updated_date = GETDATE()

        WHEN NOT MATCHED THEN
            INSERT (
                seller_id,
                seller_zip_code_prefix,
                seller_city,
                seller_state,
                created_date,
                updated_date
            )
            VALUES (
                source.seller_id,
                source.seller_zip_code_prefix,
                source.seller_city,
                source.seller_state,
                GETDATE(),
                GETDATE()
            );

        SET @row_count = @@ROWCOUNT;

        -- SUCCESS LOG
        INSERT INTO etl.etl_logs (
            process_name, layer, status, rows_processed
        )
        VALUES (
            'usp_load_dim_seller', 'Gold', 'SUCCESS', @row_count
        );

    END TRY
    BEGIN CATCH

        -- ERROR LOG
        INSERT INTO etl.etl_logs (
            process_name, layer, status, error_message
        )
        VALUES (
            'usp_load_dim_seller', 'Gold', 'FAILED', ERROR_MESSAGE()
        );

        THROW;

    END CATCH
END;
GO










