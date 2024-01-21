ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE float
USING CASE WHEN longitude = 'N/A' THEN NULL ELSE longitude::float END,
ALTER COLUMN locality TYPE varchar(255),
ALTER COLUMN store_code TYPE varchar(12),
ALTER COLUMN staff_numbers TYPE smallint USING staff_numbers::smallint,
ALTER COLUMN opening_date TYPE date
USING opening_date::date,
ALTER COLUMN store_type TYPE varchar(255),
ALTER COLUMN latitude TYPE float
USING latitude::float,
ALTER COLUMN country_code TYPE varchar(2),
ALTER COLUMN continent TYPE varchar(255);
