-- Cast the columns of the 'orders_table' to the intended data types
ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE UUID USING CAST(date_uuid as UUID),
ALTER COLUMN user_uuid TYPE UUID USING CAST(user_uuid as UUID),
ALTER COLUMN card_number TYPE VARCHAR(19),
ALTER COLUMN store_code TYPE VARCHAR(12),
ALTER COLUMN product_code TYPE VARCHAR(11),
ALTER COLUMN product_quantity TYPE smallint USING product_quantity::smallint;

-- Cast the columns of the 'dim_users' to the intended data types
ALTER TABLE dim_users
ALTER COLUMN first_name TYPE varchar(255),
ALTER COLUMN last_name TYPE varchar(255),
ALTER COLUMN date_of_birth TYPE date USING date_of_birth::date,
ALTER COLUMN country_CODE TYPE varchar(2),
ALTER COLUMN user_uuid TYPE UUID USING CAST(user_uuid as UUID),
ALTER COLUMN join_date TYPE date
USING join_date::date;

-- Cast the columns of the 'dim_store_details' to the intended data types
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

-- Remove '£' from 'product_price' and create 'weight_class' column based on weight range
UPDATE dim_products
SET product_price = REPLACE(product_price, '£', '');

ALTER TABLE dim_products ADD COLUMN weight_class varchar(14);

UPDATE dim_products
SET weight_class = CASE
    WHEN weight < 2 THEN 'Light'
    WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
	WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
    ELSE 'Truck_Required'
END;

-- Modidy and cast the columns of the 'dim_products' to the intended names and data types

ALTER TABLE dim_products
    RENAME removed TO still_available;

UPDATE dim_products
SET still_available = CASE
    WHEN still_available = 'Still_avaliable' THEN TRUE
    WHEN still_available = 'Removed' THEN FALSE
END;

ALTER TABLE dim_products
ALTER COLUMN product_price  TYPE float
USING product_price::float,
ALTER COLUMN weight  TYPE float
USING weight::float,
ALTER COLUMN "EAN" TYPE varchar(13),
ALTER COLUMN product_code TYPE varchar(11),
ALTER COLUMN date_added TYPE date USING date_added::date,
ALTER COLUMN "uuid" TYPE UUID USING CAST("uuid" as UUID),
ALTER COLUMN still_available TYPE bool
USING (still_available::bool),
ALTER COLUMN weight_class type varchar(14);

-- Cast the columns of the 'dim_date_times' to the intended data types
ALTER TABLE dim_date_times
ALTER COLUMN "month" TYPE varchar(2),
ALTER COLUMN "year" TYPE varchar(4),
ALTER COLUMN "day" TYPE varchar(2),
ALTER COLUMN "time_period" TYPE varchar(10),
ALTER COLUMN date_uuid TYPE UUID USING CAST(date_uuid as UUID);

-- Cast the columns of the 'dim_card_details' to the intended data types
ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE varchar(19),
ALTER COLUMN expiry_date TYPE varchar(5),
ALTER COLUMN date_payment_confirmed TYPE date;

-- Create primary keys on all available tables starting with 'dim'
ALTER TABLE dim_users ADD PRIMARY KEY (user_uuid);
ALTER TABLE dim_store_details ADD PRIMARY KEY (store_code);
ALTER TABLE dim_products ADD PRIMARY KEY (product_code);
ALTER TABLE dim_date_times ADD PRIMARY KEY (date_uuid);
ALTER TABLE dim_card_details ADD PRIMARY KEY (card_number);

-- Create foreign keys in 'orders_table'
ALTER TABLE orders_table
ADD CONSTRAINT fk_order_card_number
FOREIGN KEY (card_number)
REFERENCES dim_card_details (card_number);

ALTER TABLE orders_table
ADD CONSTRAINT fk_order_date_uuid
FOREIGN KEY (date_uuid)
REFERENCES dim_date_times(date_uuid);

ALTER TABLE orders_table
ADD CONSTRAINT fk_order_product_code
FOREIGN KEY (product_code)
REFERENCES dim_products (product_code);

ALTER TABLE orders_table
ADD CONSTRAINT fk_order_store_code
FOREIGN KEY (store_code)
REFERENCES dim_store_details (store_code);

ALTER TABLE orders_table
ADD CONSTRAINT fk_order_user_uuid
FOREIGN KEY (user_uuid)
REFERENCES dim_users(user_uuid);