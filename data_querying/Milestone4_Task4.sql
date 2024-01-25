SELECT
  COUNT(product_code) AS number_of_sales,
  SUM(product_quantity) AS product_quantity_count,
  CASE WHEN store_code = 'WEB-1388012W' THEN 'Web' ELSE 'Offline' END AS location
FROM orders_table
GROUP BY CASE WHEN store_code = 'WEB-1388012W' THEN 'Web' ELSE 'Offline' END
ORDER BY number_of_sales;