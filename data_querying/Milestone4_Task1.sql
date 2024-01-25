SELECT country_code AS country, COUNT(store_code) AS total_no_stores
FROM dim_store_details
WHERE locality != 'N/A' -- Not counting WEB store
GROUP BY country_code
ORDER BY total_no_stores DESC;