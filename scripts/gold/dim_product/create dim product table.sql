CREATE TABLE gold.dim_product (
    product_key INT IDENTITY(1,1) PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL,
    product_category_name VARCHAR(100),
    product_category_name_english VARCHAR(100),
    product_name_length INT,
    product_description_length INT,
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm DECIMAL(10,2),
    product_height_cm DECIMAL(10,2),
    product_width_cm DECIMAL(10,2),

    start_date DATETIME NOT NULL,
    end_date DATETIME NULL,
    is_current BIT NOT NULL
);


