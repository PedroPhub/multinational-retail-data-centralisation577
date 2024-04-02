-- Show the number of stores in each country
SELECT country_code AS country, COUNT(store_code) AS total_no_stores
FROM dim_store_details
WHERE locality != 'N/A' -- Not counting WEB store
GROUP BY country_code
ORDER BY total_no_stores DESC;

-- Show which locations have the most stores
SELECT locality, COUNT (store_code) AS total_no_stores
FROM dim_store_details AS locality
WHERE locality IN ('Chapletown','Belper', 'Bushey', 'Exeter','High Wycombe', 'Arbroath', 'Rutherglen')
GROUP BY locality
ORDER BY total_no_stores DESC;

-- Display which months produced the largest amount of sales
SELECT ROUND(SUM(product_price * product_quantity)::numeric, 2) AS total_sales, month
FROM dim_date_times
JOIN orders_table ON dim_date_times.date_uuid = orders_table.date_uuid
JOIN dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY month
ORDER BY total_sales DESC
LIMIT 6;

-- Display how many sales are made online and offline
SELECT
  COUNT(product_code) AS number_of_sales,
  SUM(product_quantity) AS product_quantity_count,
  CASE WHEN store_code = 'WEB-1388012W' THEN 'Web' ELSE 'Offline' END AS location
FROM orders_table
GROUP BY CASE WHEN store_code = 'WEB-1388012W' THEN 'Web' ELSE 'Offline' END
ORDER BY number_of_sales;

-- Display the value and percentage of sales in each type of store
SELECT 
	dim_store_details.store_type, 
	ROUND(SUM(product_price * product_quantity)::numeric, 2) AS total_sales,
	ROUND(COUNT(orders_table.product_code) * 100.0 / (SELECT COUNT(*) FROM orders_table), 2) AS percentage_of_sales
FROM orders_table
JOIN dim_products ON orders_table.product_code = dim_products.product_code
JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
GROUP BY store_type
ORDER BY total_sales DESC;

-- Display the month of each year with the highest sales
SELECT 
	ROUND(SUM(product_price * product_quantity)::numeric, 2) AS total_sales,
	dim_date_times.year,
	dim_date_times.month
FROM orders_table
JOIN dim_products ON orders_table.product_code = dim_products.product_code
JOIN dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
GROUP BY year,month
ORDER BY total_sales DESC
LIMIT 10;

-- Display staff headcount by country
SELECT 
	SUM(staff_numbers) as total_staff_numbers,
	country_code
FROM dim_store_details
GROUP BY country_code
ORDER BY total_staff_numbers DESC;

-- Display the type of stores in Germany and their total sales
SELECT 
	ROUND(SUM(product_price * product_quantity)::numeric, 2) AS total_sales,
	store_type,
	country_code
FROM orders_table
JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
JOIN dim_products ON orders_table.product_code = dim_products.product_code
WHERE country_code = 'DE'
GROUP BY store_type, country_code
ORDER BY total_sales;

-- Display how quickly, per year, a sale is being made

ALTER TABLE dim_date_times -- Create a new column with timestamp format from year, month, day, timestamp columns
ADD initial_timestamp TIMESTAMP;

UPDATE dim_date_times
SET initial_timestamp = to_timestamp(CONCAT(year, '-', LPAD(month, 2, '0'), '-', LPAD(day, 2, '0'), ' ', "timestamp"), 'YYYY-MM-DD HH24:MI:SS');



SELECT 
	year,
	'"hours": ' || TO_CHAR(interval_time_taken,'HH')::integer || ',' || 
	' "minutes": ' || TO_CHAR(interval_time_taken,'MI')::integer || ',' ||  
	' "seconds": ' || TO_CHAR(interval_time_taken,'SS')::integer || ',' ||
	' "milliseconds": ' || TO_CHAR(interval_time_taken,'MS')::integer as actual_time_taken
    -- Formatted output text. Cast to integer to remove first digit when is 0 
FROM(
		SELECT -- Get interval with average time taken between each sale
		  year,
		  AVG(initial_timestamp - next_timestamp) AS interval_time_taken 
		FROM (
			  SELECT -- Get initial_timestamp and next_timestamp using LEAD()
				year,
				initial_timestamp,
				LEAD(initial_timestamp) OVER (ORDER BY initial_timestamp DESC) AS next_timestamp
			  FROM dim_date_times
		)
		GROUP BY year
		ORDER BY interval_time_taken DESC
)
LIMIT 5;