-- check bronze layer silver layer orders table

-- 1.order id column
SELECT 
	order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date

FROM bronze.orders
WHERE order_id != TRIM(order_id);

-- 2. customer id column

SELECT 
	order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date

FROM bronze.orders
WHERE TRIM(customer_id) != customer_id


-- 3. order status column
SELECT 
    DISTINCT(order_status),
    order_status
FROM bronze.orders

-- 4. order_purchase_timestamp column

SELECT
    TRY_CAST(order_purchase_timestamp AS DATETIME2)
FROM bronze.orders
WHERE TRY_CAST(order_purchase_timestamp AS DATETIME2) IS NULL;

-- 5. order_approved_at  column

SELECT
    TRY_CAST(order_approved_at AS DATETIME2)
FROM bronze.orders
WHERE TRY_CAST(order_approved_at AS DATETIME2) IS NULL;

-- 6. order_delivered_carrier_date  column

SELECT
    TRY_CAST(order_delivered_carrier_date AS DATETIME2)
FROM bronze.orders
WHERE TRY_CAST(order_delivered_carrier_date AS DATETIME2) IS NULL;

-- 7. order_delivered_customer_date  column

SELECT
    TRY_CAST(order_delivered_customer_date AS DATETIME2)
FROM bronze.orders
WHERE TRY_CAST(order_delivered_customer_date AS DATETIME2) IS NULL;

-- 8. order_estimated_delivery_date  column

SELECT
    TRY_CAST(order_estimated_delivery_date AS DATETIME2)
FROM bronze.orders
WHERE TRY_CAST(order_estimated_delivery_date AS DATETIME2) IS NULL;
