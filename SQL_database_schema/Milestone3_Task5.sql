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
ALTER COLUMN weight_class type varchar(14)