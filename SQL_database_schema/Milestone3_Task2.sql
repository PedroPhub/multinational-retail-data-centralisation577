ALTER TABLE dim_users
ALTER COLUMN first_name TYPE varchar(255),
ALTER COLUMN last_name TYPE varchar(255),
ALTER COLUMN date_of_birth TYPE date USING date_of_birth::date,
ALTER COLUMN country_CODE TYPE varchar(2),
ALTER COLUMN user_uuid TYPE UUID USING CAST(user_uuid as UUID),
ALTER COLUMN join_date TYPE date
USING join_date::date;