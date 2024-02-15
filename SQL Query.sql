-- Casting columns of the orders_table to the correct data types

SELECT * FROM orders_table

-- Working out the maximum length of each column
SELECT  MAX(LENGTH(store_code)) AS store_max_length,
        MAX(LENGTH(CAST(card_number AS TEXT))) AS card_max_length,
        MAX(LENGTH(product_code)) AS product_max_length
FROM orders_table

-- Looking at the data types
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'orders_table';

-- Casting columns of the orders_table to the correct data types
ALTER TABLE orders_table
    ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid,
    ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid,
    ALTER COLUMN card_number TYPE VARCHAR(19),
    ALTER COLUMN store_code TYPE VARCHAR(12),
    ALTER COLUMN product_code TYPE VARCHAR(11),
    ALTER COLUMN product_quantity TYPE SMALLINT;



-- Casting columns of the dim_users table to the correct data types

SELECT * FROM dim_users

-- Working out the maximum length of each column
SELECT  MAX(LENGTH(country_code)) AS country_code_max_length
FROM dim_users

-- Altering the data types
ALTER TABLE dim_users
    ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid,
    ALTER COLUMN country_code TYPE VARCHAR(3),
    ALTER COLUMN first_name TYPE VARCHAR(255),
    ALTER COLUMN last_name TYPE VARCHAR(255),
    ALTER COLUMN date_of_birth TYPE DATE,
    ALTER COLUMN join_date TYPE DATE;

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'dim_users'



-- Updating and casting columns of the dim_store_details table 

SELECT * FROM dim_store_details


-- Working out the maximum length of each column
SELECT  MAX(LENGTH(store_code)) AS max_store_code_length,
        MAX(LENGTH(country_code)) AS max_country_code_length
FROM dim_store_details

-- Altering the data types
ALTER TABLE dim_store_details
    ALTER COLUMN longitude TYPE FLOAT USING longitude::FLOAT,
    ALTER COLUMN latitude TYPE FLOAT USING latitude::FLOAT,
    ALTER COLUMN locality TYPE VARCHAR(255),
    ALTER COLUMN store_code TYPE VARCHAR(12),
    ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::SMALLINT,
    ALTER COLUMN opening_date TYPE DATE USING opening_date::DATE,
    ALTER COLUMN store_type TYPE VARCHAR(255),
    ALTER COLUMN country_code TYPE VARCHAR(2),
    ALTER COLUMN continent TYPE VARCHAR(255);

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'dim_store_details'



-- Updating and casting columns of the dim_products table

SELECT * FROM dim_product_data

-- Removing the £ sign from the product_price column
UPDATE dim_product_data
SET product_price = REPLACE(product_price, '£', '')
WHERE product_price LIKE '£%';

-- Adding weight_class column
ALTER TABLE dim_product_data
ADD COLUMN weight_class VARCHAR(20);

-- Adding cases for weight class based on weight
UPDATE dim_product_data
SET weight_class = 
    CASE 
        WHEN weight_in_kg < 2 THEN 'Light'
        WHEN weight_in_kg >= 2 AND weight_in_kg < 40 THEN 'Mid_Sized'
        WHEN weight_in_kg >= 40 AND weight_in_kg < 140 THEN 'Heavy'
        ELSE 'Truck_Required'
    END;

-- Renaming removed column to still_available
ALTER TABLE dim_product_data
RENAME COLUMN removed TO still_available;

-- Max length of EAN, product_code and weight_class
SELECT  MAX(LENGTH(product_code)) AS max_product_code_length,
        MAX(LENGTH("EAN")) AS max_EAN_length,
        MAX(LENGTH(weight_class)) AS max_weight_class_length
FROM dim_product_data;

-- Change still_available to boolean
UPDATE dim_product_data
SET still_available =
    CASE 
        WHEN still_available = "still_available"THEN TRUE
        ELSE FALSE
    END;

-- Casting columns of the dim_products table to the correct data types
ALTER TABLE dim_product_data
    ALTER COLUMN uuid TYPE uuid USING uuid::uuid,
    ALTER COLUMN product_code TYPE VARCHAR(11),
    ALTER COLUMN product_price TYPE FLOAT USING product_price::FLOAT,
    ALTER COLUMN weight_in_kg TYPE FLOAT USING weight_in_kg::FLOAT,
    ALTER COLUMN weight_in_kg TYPE FLOAT USING weight_in_kg::FLOAT,
    ALTER COLUMN weight_class TYPE VARCHAR(14),
    ALTER COLUMN still_available TYPE BOOLEAN USING still_available::BOOLEAN,
    ALTER COLUMN date_added TYPE DATE USING date_added::DATE,
    ALTER COLUMN "EAN" TYPE VARCHAR(17);

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'dim_product_data'

-- Changing data types on dim_date_times

-- Workout max length of year, month, day and time_period
SELECT  MAX(LENGTH(year)) AS max_year_length,
        MAX(LENGTH(month)) AS max_month_length,
        MAX(LENGTH(day)) AS max_day_length,
        MAX(LENGTH(time_period)) AS max_time_period
FROM dim_date_times

-- Altering data types
ALTER TABLE dim_date_times
    ALTER COLUMN month TYPE VARCHAR(10),
    ALTER COLUMN day TYPE VARCHAR(10),
    ALTER COLUMN year TYPE VARCHAR(10),
    ALTER COLUMN time_period TYPE VARCHAR(10),
    ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid;


-- Updating columns for dim_card_details

SELECT  MAX(LENGTH(card_number)) AS max_card_number_length,
        MAX(LENGTH(expiry_date)) AS max_expiry_date_length
FROM dim_card_details

ALTER TABLE dim_card_details
    ALTER COLUMN expiry_date TYPE VARCHAR(10),
    ALTER COLUMN card_number TYPE VARCHAR(22),
    ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::DATE;

-- Set primary keys to each table

ALTER TABLE dim_card_details ADD PRIMARY KEY (card_number);

ALTER TABLE dim_date_times ADD PRIMARY KEY (date_uuid);

ALTER TABLE dim_product_data ADD PRIMARY KEY (product_code);

ALTER TABLE dim_store_details ADD PRIMARY KEY (store_code);

ALTER TABLE dim_users ADD PRIMARY KEY (user_uuid);

-- Set foreign keys
ALTER TABLE orders_table
    ALTER COLUMN card_number TYPE VARCHAR(22),
    ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid,
    ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid,
    ALTER COLUMN product_code TYPE VARCHAR(11),
    ALTER COLUMN store_code TYPE VARCHAR(12);

-- Deleting null values
DELETE FROM orders_table
WHERE card_number NOT IN 
    (SELECT card_number FROM dim_card_details);

DELETE FROM orders_table
WHERE date_uuid NOT IN 
    (SELECT date_uuid FROM dim_date_times);

DELETE FROM orders_table
WHERE product_code NOT IN 
    (SELECT product_code FROM dim_product_data);

DELETE FROM orders_table
WHERE store_code NOT IN 
    (SELECT store_code FROM dim_store_details);

DELETE FROM orders_table
WHERE user_uuid NOT IN 
    (SELECT user_uuid FROM dim_users);

-- Adding foreign keys
ALTER TABLE orders_table
    ADD CONSTRAINT fk_card_number 
    FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number);

ALTER TABLE orders_table
    ADD CONSTRAINT fk_date_uuid 
    FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid);

ALTER TABLE orders_table
    ADD CONSTRAINT fk_product_code 
    FOREIGN KEY (product_code) REFERENCES dim_product_data(product_code);

ALTER TABLE orders_table
    ADD CONSTRAINT fk_store_code 
    FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code);

ALTER TABLE orders_table
    ADD CONSTRAINT fk_user_uuid 
    FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid);


-- Remove foreign keys
ALTER TABLE orders_table
    DROP CONSTRAINT fk_card_number,
    DROP CONSTRAINT fk_date_uuid,
    DROP CONSTRAINT fk_product_code,
    DROP CONSTRAINT fk_store_code,
    DROP CONSTRAINT fk_user_uuid;


-- Total number of stores in each country

SELECT country_code, COUNT(store_code) AS number_of_stores
FROM dim_store_details
GROUP BY country_code
ORDER BY number_of_stores DESC;

-- Which locations currently have the most stores

SELECT locality, COUNT(store_code) AS total_no_stores
FROM dim_store_details
GROUP BY locality
ORDER BY total_no_stores DESC
LIMIT 10

-- Which months produced the most amount of sales

SELECT  ROUND(CAST(SUM(dim_product_data.product_price * orders_table.product_quantity) AS NUMERIC), 2) AS total_sales,
        dim_date_times.month AS month
FROM dim_product_data
INNER JOIN
    orders_table
    ON orders_table.product_code = dim_product_data.product_code
INNER JOIN
    dim_date_times
    ON orders_table.date_uuid = dim_date_times.date_uuid
GROUP BY month
ORDER BY total_sales DESC;

-- How many sales are coming from online than offline

SELECT  COUNT(orders_table.date_uuid) AS number_of_sales,
        SUM(orders_table.product_quantity) AS product_quantity_count,
        CASE
            WHEN dim_store_details.store_code LIKE 'WEB%' THEN 'Online'
            ELSE 'Offline'
        END as location
FROM
    dim_store_details
INNER JOIN
    orders_table
    ON orders_table.store_code = dim_store_details.store_code
GROUP BY location;


-- Percentage of sales through each store type

WITH TotalSales AS(
    SELECT COUNT(date_uuid) AS total_no_sales
    FROM orders_table
)
SELECT  dim_store_details.store_type,
        ROUND(CAST(SUM(dim_product_data.product_price*orders_table.product_quantity)AS NUMERIC), 2) AS total_sales,
        ROUND((CAST(COUNT(orders_table.date_uuid)AS NUMERIC) * 100) / (
            SELECT total_no_sales
            FROM TotalSales
        ), 2) AS percentage_total
FROM
    orders_table
INNER JOIN
    dim_store_details ON orders_table.store_code = dim_store_details.store_code
INNER JOIN
    dim_product_data ON orders_table.product_code = dim_product_data.product_code
GROUP BY dim_store_details.store_type
ORDER BY total_sales DESC

-- Which month in each year made the highest cost of sales

SELECT ROUND(CAST(SUM(orders_table.product_quantity * dim_product_data.product_price) AS NUMERIC), 2) AS total_sales,
       dim_date_times.year AS year,
       dim_date_times.month AS month
FROM dim_date_times
INNER JOIN
    orders_table ON orders_table.date_uuid = dim_date_times.date_uuid
INNER JOIN
    dim_product_data ON dim_product_data.product_code = orders_table.product_code
GROUP BY year, month
ORDER BY total_sales DESC;

-- What is the staff headcount

SELECT  SUM(staff_numbers) AS total_staff_numbers,
        country_code

FROM dim_store_details
GROUP BY country_code
ORDER BY total_staff_numbers DESC;

-- Which type of store is generating the most sales in Germany?

SELECT  ROUND(CAST(SUM(orders_table.product_quantity * dim_product_data.product_price) AS NUMERIC), 2) AS total_sales,
        store_type,
        country_code

FROM dim_store_details

INNER JOIN
orders_table ON orders_table.store_code = dim_store_details.store_code
INNER JOIN
dim_product_data ON orders_table.product_code = dim_product_data.product_code
WHERE country_code = 'DE'
GROUP BY store_type, country_code
ORDER BY total_sales DESC;

-- How quickly is the company making sales

WITH purchase_time_cte AS(
                SELECT
                        year,
                        month,
                        day,
                        timestamp,
                        CAST(CONCAT(year, '-', month, '-', day, ' ', timestamp) AS TIMESTAMP) AS purchase_time
                FROM
                        dim_date_times
                ORDER BY
                        year, month, day, timestamp
            ),
            next_purchase_time_cte AS(
                SELECT
                        year,
                        purchase_time,
                        LEAD(purchase_time) OVER (PARTITION BY year ORDER BY purchase_time) AS next_purchase_time
                FROM
                        purchase_time_cte
                ORDER BY
                        year, purchase_time
            ),
            purchase_time_difference_cte AS (
                SELECT
                        year,
                        purchase_time,
                        next_purchase_time,
                        EXTRACT(EPOCH FROM(next_purchase_time - purchase_time)) AS purchase_time_difference
                FROM
                        next_purchase_time_cte
                ORDER BY
                        year, purchase_time, next_purchase_time
            )

            SELECT
                    year,
                    CONCAT(
                    '"hours": ', FLOOR(AVG(purchase_time_difference) / 3600), ', ',
                    '"minutes": ', FLOOR((AVG(purchase_time_difference) % 3600) / 60), ', ',
                    '"seconds": ', ROUND(AVG(purchase_time_difference) % 60), ', ',
                    '"milliseconds": ', ROUND((AVG(purchase_time_difference)*1000)%1000)
                    ) AS actual_time_taken
            FROM
                    purchase_time_difference_cte
            GROUP BY
                    year
            ORDER BY
                    AVG(purchase_time_difference) DESC;