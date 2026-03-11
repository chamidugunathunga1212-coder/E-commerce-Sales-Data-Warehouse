SELECT 
	order_id
FROM bronze.payments
WHERE order_id = '' OR order_id IS NULL OR TRIM(order_id) != order_id;


SELECT 
	DISTINCT payment_type
FROM bronze.payments;

SELECT 
	payment_type,COUNT(*),
	CASE 
		WHEN LOWER(TRIM(payment_type)) =  'credit_card' THEN 'credit_card'
		WHEN LOWER(TRIM(payment_type)) =  'debit_card' THEN 'debit_card'
		WHEN LOWER(TRIM(payment_type)) =  'voucher' THEN 'voucher'
		WHEN LOWER(TRIM(payment_type)) =  'boleto' THEN 'boleto'
		ELSE 'n/a'
	END AS payment_type
FROM bronze.payments
GROUP BY payment_type;


SELECT 
	payment_installments
FROM bronze.payments
WHERE payment_installments = '' OR payment_installments IS NULL OR TRIM(payment_installments) != payment_installments;

SELECT 
	DISTINCT payment_installments
FROM bronze.payments;



SELECT * FROM silver.payments;
