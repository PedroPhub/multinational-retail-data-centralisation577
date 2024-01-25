SELECT 
	dim_store_details.store_type, 
	ROUND(SUM(product_price * product_quantity)::numeric, 2) AS total_sales,
	ROUND(COUNT(orders_table.product_code) * 100.0 / (SELECT COUNT(*) FROM orders_table), 2) AS percentage_of_sales
FROM orders_table
JOIN dim_products ON orders_table.product_code = dim_products.product_code
JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
GROUP BY store_type
ORDER BY total_sales DESC