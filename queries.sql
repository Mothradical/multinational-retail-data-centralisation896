-- Task 1: How many stores does the business have and in which countries? --

SELECT
    country_code AS country,
    COUNT(address) AS total_no_stores
FROM
    dim_store_details
GROUP BY
    country
ORDER BY
    total_no_stores DESC;

-- Task 2: Which locations currently have the most stores?

SELECT
    locality,
    COUNT(address) AS total_no_stores
FROM
    dim_store_details
GROUP BY
    locality
ORDER BY
    total_no_stores DESC
LIMIT 7;

-- Task 3: Which months produced the largest amount of sales?

WITH cte AS (
	SELECT dim_products.product_price * orders_table.product_quantity AS total_sales_amount,
    	orders_table.date_uuid AS date_uuid
	FROM
    	orders_table
	INNER JOIN
    	dim_products ON orders_table.product_code = dim_products.product_code
		)

SELECT SUM(total_sales_amount) AS total_sales_amount,
	dim_date_times.month
FROM
	cte
INNER JOIN
	dim_date_times ON cte.date_uuid = dim_date_times.date_uuid
GROUP BY
	dim_date_times.month
ORDER BY
	total_sales_amount DESC
LIMIT 6;

-- Task 4: How many sales are coming from online?

ALTER TABLE dim_store_details
ADD COLUMN "location" VARCHAR(7);

UPDATE dim_store_details
SET "location" = 'Web'
WHERE store_type = 'Web Portal';

UPDATE dim_store_details
SET "location" = 'Offline'
WHERE store_type != 'Web Portal';

SELECT COUNT(orders_table.date_uuid) AS "number of sales",
    SUM(orders_table.product_quantity) AS product_quantity_count,
    dim_store_details."location"
FROM
    dim_store_details
INNER JOIN
    orders_table ON orders_table.store_code = dim_store_details.store_code
GROUP BY
    "location"
ORDER BY
    product_quantity_count;

ALTER TABLE dim_store_details
DROP COLUMN "location";

-- Task 5: What parcentage of sales come through each type of store?

WITH cte AS (
	SELECT dim_products.product_price * orders_table.product_quantity AS total_sales_amount,
    	dim_store_details.store_type AS store_type,
        orders_table.date_uuid AS date_uuid
	FROM
    	orders_table
	INNER JOIN
    	dim_products ON orders_table.product_code = dim_products.product_code
    INNER JOIN
        dim_store_details ON orders_table.store_code = dim_store_details.store_code
		)

SELECT SUM(total_sales_amount) AS total_sales,
	store_type,
    ROUND(((COUNT(store_type) * 100.0) / 120123), 2) AS "sales_made(%)"
FROM
	cte
GROUP BY
    store_type
ORDER BY
    total_sales DESC;

-- Task 6: Which month in each year produced the highest cost of sales?

WITH cte AS (
	SELECT dim_products.product_price * orders_table.product_quantity AS total_sales_amount,
    	orders_table.date_uuid AS date_uuid
	FROM
    	orders_table
	INNER JOIN
    	dim_products ON orders_table.product_code = dim_products.product_code
		)

SELECT SUM(total_sales_amount) AS total_sales,
	dim_date_times.year,
	dim_date_times.month
FROM
	cte
INNER JOIN
	dim_date_times ON cte.date_uuid = dim_date_times.date_uuid
GROUP BY
	dim_date_times.month, dim_date_times.year
ORDER BY
	total_sales DESC
LIMIT 10;

-- Task 7: What is our staff headcount?

SELECT SUM(staff_numbers) AS total_staff_numbers,
	country_code
FROM
	dim_store_details
GROUP BY
	country_code
ORDER BY
	total_staff_numbers DESC;

-- Task 8: Which German store type is selling the most?

WITH cte AS (
	SELECT dim_products.product_price * orders_table.product_quantity AS total_sales_amount,
		dim_store_details.store_type,
		dim_store_details.country_code
	FROM
    	orders_table
	INNER JOIN
   	dim_products ON orders_table.product_code = dim_products.product_code
	INNER JOIN
		dim_store_details ON orders_table.store_code = dim_store_details.store_code
		)
SELECT SUM(total_sales_amount) as total_sales,
	store_type,
	country_code
FROM
	cte
GROUP BY
	store_type, country_code
HAVING
	country_code = 'DE'
ORDER BY
	total_sales;

-- Task 9: How quickly is the company making sales?

WITH cte AS (
	SELECT "year",
		CONCAT(year, '/', month, '/', day,' ', timestamp) AS datetime
	FROM
		dim_date_times
	ORDER BY
		datetime
		)
SELECT "year",
	AVG(actual_time_taken) AS actual_time_taken
FROM
	(SELECT "year",
		LEAD(datetime) OVER (ORDER BY "datetime") - datetime AS actual_time_taken
	FROM (
		SELECT "year",
			CAST(datetime AS timestamp) AS datetime
		FROM cte
		)
	)
GROUP BY
	"year"
ORDER BY
	"year" DESC;