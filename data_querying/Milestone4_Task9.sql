-- Create a new column with timestamp format from year, month, day, timestamp columns
/*(ALTER TABLE dim_date_times
ADD initial_timestamp TIMESTAMP;

UPDATE dim_date_times
SET initial_timestamp = to_timestamp(CONCAT(year, '-', LPAD(month, 2, '0'), '-', LPAD(day, 2, '0'), ' ', "timestamp"), 'YYYY-MM-DD HH24:MI:SS');
*/


SELECT 
	year,
	'"hours": ' || TO_CHAR(interval_time_taken,'HH')::integer || ',' || -- Formatting output text. Cast to integer to remove first digit when is 0
	' "minutes": ' || TO_CHAR(interval_time_taken,'MI')::integer || ',' ||  
	' "seconds": ' || TO_CHAR(interval_time_taken,'SS')::integer || ',' ||
	' "milliseconds": ' || TO_CHAR(interval_time_taken,'MS')::integer as actual_time_taken 
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
LIMIT 5 -- Optional to match Output for the task