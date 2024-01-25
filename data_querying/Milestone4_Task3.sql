SELECT ROUND(SUM(product_price * product_quantity)::numeric, 2) AS total_sales, month
FROM dim_date_times
JOIN orders_table ON dim_date_times.date_uuid = orders_table.date_uuid
JOIN dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY month
ORDER BY total_sales DESC
LIMIT 6;