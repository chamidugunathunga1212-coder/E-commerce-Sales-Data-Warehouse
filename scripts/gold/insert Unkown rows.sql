SET IDENTITY_INSERT gold.dim_customer ON;

IF NOT EXISTS (SELECT 1 FROM gold.dim_customer WHERE customer_key = 0)
BEGIN
    SET IDENTITY_INSERT gold.dim_customer ON;

    INSERT INTO gold.dim_customer (
        customer_key,
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        created_date,
        updated_date
    )
    VALUES (0, 'UNKNOWN', 'UNKNOWN', 0, 'UNKNOWN', 'UN', GETDATE(), GETDATE());

    SET IDENTITY_INSERT gold.dim_customer OFF;
END

Go



SET IDENTITY_INSERT gold.dim_product ON;
INSERT INTO gold.dim_product (
    product_key,
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
)
VALUES (0, 'UNKNOWN', 'UNKNOWN', 0, 0, 0,0,0,0,0,0);

SET IDENTITY_INSERT gold.dim_product OFF;
GO







SET IDENTITY_INSERT gold.dim_seller ON;
INSERT INTO gold.dim_seller (
    seller_key,
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
)
VALUES (0, 'UNKNOWN', 0, 'UNKNOWN','UN');

SET IDENTITY_INSERT gold.dim_seller OFF;
GO





INSERT INTO gold.dim_date (
    date_key,
    full_date,
    day_number, 
    month_number, 
    month_name,
    quarter_number,
    year_number,
    weekday_name
)
VALUES (0, '1900-01-01', 1, 1, 'Unknown', 1, 1900, 'Unknown');
GO


IF NOT EXISTS (SELECT 1 FROM gold.dim_seller WHERE seller_key = 0)
BEGIN
    SET IDENTITY_INSERT gold.dim_seller ON;

    INSERT INTO gold.dim_seller (
        seller_key,
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state,
        created_date,
        updated_date
    )
    VALUES (0, 'UNKNOWN', 0, 'UNKNOWN', 'UN', GETDATE(), GETDATE());

    SET IDENTITY_INSERT gold.dim_seller OFF;
END


GO



SELECT *
FROM gold.dim_customer;

SELECT *
FROM gold.dim_product;


SELECT *
FROM gold.dim_seller;
